# Issue #483 Trace Analysis and Expected Output Rules

## User Symbol Representation Rules

### Section 3 (User accounts and pool mappings)
- Each user receives a single-character ID for job representation
- User IDs are assigned sequentially from the character set: `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`
- When more than 62 unique users exist (exceeding available characters), the "long tail" users are represented with the `?` character

### Core Allocation Symbols
- `_` (underscore): Free/unallocated core
- `#` (hash): Non-existent core on worker node (when nodes have different core counts)
- `?` (question mark): Jobs from users in the "long tail" (beyond the 62-character limit)
- Alphanumeric characters (0-9, A-Z, a-z): Jobs from identified users

### `-4` Option Semantics
The `-4` option reverses the default behavior:
- **Default behavior**: Show jobs from users with the most cores first
- **With `-4`**: Show jobs from users with the fewest cores first
- This affects both the user ID assignment order and the display priority in Section 3

## Expected Output Validation Rules

### Section 3 Consistency Requirements
1. Every user symbol in the worker node matrix must have a corresponding entry in Section 3
2. The job counts in Section 3 must match the number of cores assigned to each user in the matrix
3. User symbols must be consistently applied across all worker nodes
4. The `?` symbol aggregates all "long tail" users and their combined statistics

### Trace Handling Contract
When processing cluster traces:
1. Parse all unique user accounts from job data
2. Assign single-character IDs in order of core usage (high to low, or reversed with `-4`)
3. Users beyond the 62-character limit are grouped under `?`
4. Ensure Section 3 displays accurate job counts and user information
5. Maintain symbol consistency across matrix visualization

## Implementation Notes
- The user symbol assignment is deterministic based on current core usage
- Long tail grouping prevents matrix readability issues with hundreds of users
- The `?` character provides a bounded representation while maintaining cluster overview capability