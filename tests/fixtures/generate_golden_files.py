#!/usr/bin/env python3
"""
Script to generate golden files for the deterministic multi-partition test.
"""

import json
import os
import sys

# Base directory for golden files
BASE_DIR = "tests/fixtures/expected_outputs/deterministic_multi_partition"

# Message distribution
PARTITION_DISTRIBUTION = {
    0: [0, 3, 6, 9],  # Partition 0: messages 0, 3, 6, 9 (4 messages)
    1: [1, 4, 7],     # Partition 1: messages 1, 4, 7 (3 messages)
    2: [2, 5, 8],     # Partition 2: messages 2, 5, 8 (3 messages)
}

def string_to_byte_array(s):
    """Convert a string to an array of byte values."""
    return [ord(c) for c in s]

def generate_message(message_id, partition, offset):
    """Generate a message with the given ID, partition, and offset."""
    # Create key with format "message-key-00X"
    key_str = f"message-key-{message_id:03}"
    key_bytes = string_to_byte_array(key_str)
    
    # Create JSON value with deterministic content
    value_obj = {
        "id": message_id,
        "message": f"Simple message number {message_id}",
        "timestamp": "2025-01-01T00:00:00Z",
        "sequence": message_id
    }
    value_str = json.dumps(value_obj)
    value_bytes = string_to_byte_array(value_str)
    
    # Create the message object
    message = {
        "key": key_bytes,
        "value": value_bytes,
        "headers": {},
        "topic": "test-deterministic-uuid",  # Actual topic name will be different but doesn't matter for comparison
        "partition": partition,
        "offset": offset,
        "timestamp": 1714500000000  # Fixed timestamp for deterministic output
    }
    
    return message

def main():
    """Generate the golden files."""
    # Create the base directory if it doesn't exist
    os.makedirs(BASE_DIR, exist_ok=True)
    
    # Generate files for each partition
    for partition, message_ids in PARTITION_DISTRIBUTION.items():
        # Create the partition directory if it doesn't exist
        partition_dir = os.path.join(BASE_DIR, f"partition-{partition}")
        os.makedirs(partition_dir, exist_ok=True)
        
        # Generate a file for each message
        for offset, message_id in enumerate(message_ids):
            # Generate the message
            message = generate_message(message_id, partition, offset)
            
            # Write the message to a file
            filename = os.path.join(partition_dir, f"{offset:09d}.json")
            with open(filename, "w") as f:
                json.dump(message, f, indent=2)
            
            print(f"Generated {filename}")

if __name__ == "__main__":
    main()