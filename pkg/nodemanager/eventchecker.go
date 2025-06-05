/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nodemanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	utilnode "sigs.k8s.io/cloud-provider-azure/pkg/util/node"
)

const (
	metadataURL   = "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01"
	conditionType = "ScheduledEvent"
)

// EventChecker periodically checks for Azure scheduled events and updates the Kubernetes node status accordingly.
type EventChecker struct {
	nodeName   string
	kubeClient clientset.Interface
	recorder   record.EventRecorder
	httpClient *http.Client

	updateFrequency time.Duration
}

// EventSource represents the source of the event, which can be either "Platform" or "User".
type EventSource string

// EventType represents the type of the event, such as "Freeze", "Reboot", "Terminate", etc.
type EventType string

const (
	EventSourcePlatform EventSource = "Platform"
	EventSourceUser     EventSource = "User"

	EventTypeFreeze    EventType = "Freeze"
	EventTypeReboot    EventType = "Reboot"
	EventTypeTerminate EventType = "Terminate"
	EventTypePreempt   EventType = "Preempt"
	EventTypeRedeploy  EventType = "Redeploy"
)

// AllowedEventSources defines the sources of events that are allowed to be processed.
var AllowedEventSources = map[EventSource]struct{}{
	EventSourcePlatform: {},
	EventSourceUser:     {},
}

// AllowedEventTypes defines the types of events that are allowed to be processed.
var AllowedEventTypes = map[EventType]struct{}{
	EventTypeFreeze:    {},
	EventTypeReboot:    {},
	EventTypeTerminate: {},
	EventTypePreempt:   {},
	EventTypeRedeploy:  {},
}

type Event struct {
	Description       string      `json:"Description"`
	DurationInSeconds int         `json:"DurationInSeconds"`
	EventID           string      `json:"EventId"`
	EventSource       EventSource `json:"EventSource"`
	EventStatus       string      `json:"EventStatus"`
	EventType         EventType   `json:"EventType"`
	NotBefore         string      `json:"NotBefore"`
	ResourceType      string      `json:"ResourceType"`
	Resources         []string    `json:"Resources"`
}

type EventResponse struct {
	DocumentIncarnation int     `json:"DocumentIncarnation"`
	Events              []Event `json:"Events"`
}

// NewEventChecker creates a new EventChecker instance
func NewEventChecker(nodeName string, kubeClient clientset.Interface, httpClient *http.Client, updateFrequency time.Duration) *EventChecker {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 10 * time.Second, // Set a reasonable timeout for HTTP requests
		}
	}
	return &EventChecker{
		httpClient:      httpClient,
		nodeName:        nodeName,
		kubeClient:      kubeClient,
		updateFrequency: updateFrequency,
	}
}

// Run starts the event checker loop that periodically checks for Azure scheduled events
func (ec *EventChecker) Run(ctx context.Context) {
	ticker := time.NewTicker(ec.updateFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ec.CheckAzureScheduledEvents(context.Background()); err != nil {
				klog.Errorf("Failed to check Azure scheduled events: %v", err)
			}
		}
	}
}

// CheckAzureScheduledEvents queries the Azure metadata service for scheduled events
// and updates the Kubernetes node with a custom condition "NodeEvent".
func (ec *EventChecker) CheckAzureScheduledEvents(ctx context.Context) error {
	req, err := http.NewRequest("GET", metadataURL, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Metadata", "true")

	resp, err := ec.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected status code from metadata service: %d", resp.StatusCode)
	}

	var result EventResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	if len(result.Events) == 0 {
		err = utilnode.SetNodeCondition(ec.kubeClient, types.NodeName(ec.nodeName), v1.NodeCondition{
			Type:               conditionType,
			Status:             v1.ConditionFalse,
			Reason:             "NoScheduledEvents",
			Message:            "No scheduled events found",
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime:
		})

		return nil
	}

	node, err := ec.kubeClient.CoreV1().Nodes().Get(ctx, ec.nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	updated := ec.CleanUpNodeConditions(node)

	for _, event := range result.Events {
		node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
			Type:               conditionType,
			Status:             v1.ConditionTrue,
			LastTransitionTime: metav1.Time{Time: time.Now()},
			Reason:             ec.conditionReason(event),
			Message:            ec.conditionMessage(event),
		})
		updated = true
	}

	if !updated {
		return nil // No new events to update
	}

	// Update the node status with the new conditions

	// Serialize the updated node status
	statusBytes, err := json.Marshal(map[string]interface{}{
		"status": node.Status,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal node status: %w", err)
	}

	_, err = ec.kubeClient.CoreV1().Nodes().PatchStatus(ctx, ec.nodeName, statusBytes)
	return err
}

func (ec *EventChecker) CleanUpNodeConditions(node *v1.Node) bool {
	updated := false
	newConditions := []v1.NodeCondition{}
	for _, cond := range node.Status.Conditions {
		if cond.Type == conditionType {
			updated = true
			continue
		}
		newConditions = append(newConditions, cond)
	}

	node.Status.Conditions = newConditions
	return updated
}

func (ec *EventChecker) conditionReason(event Event) string {
	return string(event.EventSource) + "-" + string(event.EventType)
}

func (ec *EventChecker) conditionMessage(event Event) string {
	return fmt.Sprintf("Scheduled event: %s, NotBefore: %s, Duration: %d seconds, Description: %s", event.EventID, event.NotBefore, event.DurationInSeconds, event.Description)
}
