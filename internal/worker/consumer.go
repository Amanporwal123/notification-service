package worker

// consumer.go runs in the background listening to the Kafka queue.
// It picks up pending notifications and actually executes the sending logic (e.g., via Email/SMS API).
