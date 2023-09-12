## TrafficCond Table

This table stores information about traffic sensors in the Austin region.

| Column         | Type     | Description                                                                                 |
| -------------- | -------- | ------------------------------------------------------------------------------------------- |
| uid            | Integer  | Unique identifier                                                                           |
| id             | String   | A unique identifier associated with the traffic data                                        |
| typ            | String   | Represents the type of traffic data                                                         |
| volume         | Integer  | Indicates the volume of traffic at the sensor                                               |
| speed          | Integer  | Represents the speed of traffic at the sensor                                               |
| occupancy      | Integer  | Indicates the occupancy level of traffic                                                    |
| timestamp      | String   | Timestamp of the traffic data in string format                                              |
| timestamp_dt   | DataTime | Timestamp of the traffic data in DateTime format                                            |
| timestamp_diff | Integer  | Time difference between the current record's time and the previous record's time in seconds |

### Constraints and Notes

- **Primary Key**: The `uid` column serves as the primary key, ensuring that each record has a unique identifier.
- **Unique Constraint**: The `id` column should contain unique values to avoid data duplication.
- **Default Values**: The `timestamp_diff` column has a default value of 0, indicating that it will be set to zero if not explicitly provided during record insertion.






