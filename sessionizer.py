def sessionize_events(events):
    
    ''' 
        Time Complexity: O(N log N)  -  Sorting events per user dominates the runtime.
        Space Complexity: O(N)  -  Used to store grouped events and session results.
    '''
    # 30 minutes session gap in seconds (30 * 60)
    SESSION_GAP = 1800

    # grouping events by user
    user_events = {}

    for event in events:
        user_id = event[0]
        timestamp = event[1]
        if user_id not in user_events:
            user_events[user_id] = []
        user_events[user_id].append(timestamp)

    # create sessions for each user
    result = {}

    for user_id in user_events:
        timestamps = user_events[user_id]

        #   events may not be in order, so sorting the events   
        timestamps.sort()
        
        sessions = []
        
        start_time = timestamps[0]
        end_time = timestamps[0]
        count = 1

        for i in range(1, len(timestamps)):
            current_time = timestamps[i]

            # checking session gap
            if current_time - end_time <= SESSION_GAP:
                end_time = current_time
                count += 1
            else:
                # closing previous session
                sessions.append({
                    "start": start_time,
                    "end": end_time,
                    "count": count
                })

                # starting new session
                start_time = current_time
                end_time = current_time
                count = 1

        # adding last session
        sessions.append({
            "start": start_time,
            "end": end_time,
            "count": count
        })

        result[user_id] = sessions

    return result

if __name__ == "__main__":
    events = [
        ("user_A", 100),
        ("user_A", 110),
        ("user_B", 500),
        ("user_A", 2000),
        ("user_A", 120)
    ]

    output = sessionize_events(events)
    print(output)

