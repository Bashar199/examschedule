import mysql.connector
import json
import requests
import pandas as pd
import random
from datetime import datetime, timedelta, date
import os
import argparse
import csv
import sys
import os.path
import re
from collections import defaultdict

# Import config settings
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from fakeData.config import DEEPSEEK_API, DATABASE, SCHEDULING, OUTPUT
    print("Config loaded successfully")
except ImportError:
    print("Config file not found. Creating default config...")
    # Default configurations if config file is missing
    DEEPSEEK_API = {
        'url': 'https://api.deepseek.com/v1/chat/completions',
        'key': '',
        'model': 'deepseek-chat',
        'temperature': 0.1,
        'max_tokens': 4000
    }
    DATABASE = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'port': 3306,
        'database': 'exam_schedule'
    }
    SCHEDULING = {
        'time_slots': ["8:30-10:30", "11:30-1:30", "2:30-4:30"],
        'min_days_between_exams': 1,
        'no_same_day_exams': True,
        'scheduling_window_days': 21,
        'start_date_offset': 30,
        'skip_weekends': True
    }
    OUTPUT = {
        'default_csv_filename': 'exam_schedule_output.csv',
        'save_to_database': False
    }

# Use config values
DB_CONFIG = DATABASE
DEEPSEEK_API_URL = DEEPSEEK_API['url']
DEEPSEEK_API_KEY = DEEPSEEK_API['key']
EXAM_TIME_SLOTS = SCHEDULING['time_slots']
MIN_DAYS_BETWEEN_EXAMS = SCHEDULING['min_days_between_exams']

# --- Define specific date range (use a sensible year like 2025) ---
# You could make these configurable via SCHEDULING or command-line args
# For now, hardcoding as requested.
SPECIFIC_START_DATE = date(2025, 5, 25)
SPECIFIC_END_DATE = date(2025, 6, 13)

def get_db_connection():
    """Establish connection to MySQL database"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

def fetch_student_enrollments(connection):
    """Fetch all student enrollments from the database"""
    cursor = connection.cursor(dictionary=True)
    
    try:
        # Get all student enrollments
        cursor.execute("""
        SELECT s.id, s.name, s.department, s.academic_level, e.course_code
        FROM students s
        JOIN enrollments e ON s.id = e.student_id
        ORDER BY s.id
        """)
        
        enrollments = cursor.fetchall()
        print(f"Fetched {len(enrollments)} student-course enrollments")
        return enrollments
    except Exception as e:
        print(f"Error fetching student enrollments: {e}")
        return []
    finally:
        cursor.close()

def fetch_course_info(connection):
    """Fetch course information from all course tables"""
    cursor = connection.cursor(dictionary=True)
    courses = []
    
    try:
        # Get all tables that end with '_courses'
        cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name LIKE '%%_courses'
        """, (DB_CONFIG['database'],))
        
        course_tables = [row['table_name'] for row in cursor.fetchall()]
        
        # Fetch courses from each table
        for table in course_tables:
            cursor.execute(f"SELECT code, name FROM {table}")
            table_courses = cursor.fetchall()
            for course in table_courses:
                course['table'] = table
                courses.append(course)
        
        print(f"Fetched {len(courses)} courses from {len(course_tables)} tables")
        return courses
    except Exception as e:
        print(f"Error fetching course information: {e}")
        return []
    finally:
        cursor.close()

def group_enrollments_by_student(enrollments):
    """Group course enrollments by student"""
    students = {}
    
    for enrollment in enrollments:
        student_id = enrollment['id']
        if student_id not in students:
            students[student_id] = {
                'name': enrollment['name'],
                'department': enrollment['department'],
                'academic_level': enrollment['academic_level'],
                'courses': []
            }
        
        students[student_id]['courses'].append(enrollment['course_code'])
    
    return students

def analyze_course_conflicts(students):
    """Analyze which courses have many students in common"""
    course_pairs = {}
    
    for student_id, student_data in students.items():
        courses = student_data['courses']
        
        # Check all pairs of courses
        for i in range(len(courses)):
            for j in range(i+1, len(courses)):
                course1 = courses[i]
                course2 = courses[j]
                
                # Ensure consistent order of pair
                if course1 > course2:
                    course1, course2 = course2, course1
                
                pair = f"{course1}_{course2}"
                if pair not in course_pairs:
                    course_pairs[pair] = 0
                
                course_pairs[pair] += 1
    
    # Sort by conflict count
    sorted_pairs = sorted(course_pairs.items(), key=lambda x: x[1], reverse=True)
    
    # Print the top 20 conflicts
    print("\nTop 20 Course Conflicts:")
    for pair, count in sorted_pairs[:20]:
        course1, course2 = pair.split('_')
        print(f"{course1} and {course2}: {count} students in common")
    
    return sorted_pairs

def prepare_data_for_api(students, courses, enrollments):
    """Prepare data in a format suitable for DeepSeek API"""
    # Group courses by department and level
    courses_by_dept_level = {}
    for course in courses:
        table = course['table']
        dept, level, _ = table.split('_')
        
        key = f"{dept}_{level}"
        if key not in courses_by_dept_level:
            courses_by_dept_level[key] = []
            
        courses_by_dept_level[key].append({
            'code': course['code'],
            'name': course['name']
        })
    
    # Prepare student data with courses
    student_data = []
    for student_id, data in students.items():
        student_data.append({
            'id': student_id,
            'name': data['name'],
            'department': data['department'],
            'academic_level': data['academic_level'],
            'courses': data['courses']
        })
    
    # Analyze course conflicts for scheduling help
    conflict_analysis = []
    course_conflicts = analyze_course_conflicts(students)
    for pair, count in course_conflicts[:50]:  # Include top 50 conflicts
        course1, course2 = pair.split('_')
        conflict_analysis.append({
            'course1': course1,
            'course2': course2,
            'common_students': count
        })
    
    # Create the final data structure
    api_data = {
        'courses_by_dept_level': courses_by_dept_level,
        'students': student_data,
        'conflict_analysis': conflict_analysis,
        'constraints': {
            'time_slots': EXAM_TIME_SLOTS,
            'min_days_between_exams': MIN_DAYS_BETWEEN_EXAMS,
            'no_same_day_exams': SCHEDULING['no_same_day_exams']
        }
    }
    
    return api_data

def call_deepseek_api(api_data):
    """Call DeepSeek API with the prepared data"""
    if not DEEPSEEK_API_KEY:
        print("Warning: DeepSeek API key not set. Using local generation method instead.")
        return generate_schedule_locally(api_data)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
    }
    
    # Convert complex objects to strings for API
    api_data_json = json.dumps(api_data)
    
    # Prepare the prompt for DeepSeek - UPDATED PROMPT
    start_date_str = SPECIFIC_START_DATE.strftime("%Y-%m-%d")
    end_date_str = SPECIFIC_END_DATE.strftime("%Y-%m-%d")

    messages = [
        {"role": "system", "content": "You are an expert in exam scheduling. Your task is to create an optimal exam schedule that avoids conflicts within a specific date range, allowing multiple exams per day if student schedules permit."},
        {"role": "user", "content": f"""
I need you to create an exam schedule for the following data:
{api_data_json}

The schedule MUST follow these rules:
1. All exams MUST be scheduled between {start_date_str} and {end_date_str} (inclusive).
2. No student should have two exams scheduled on the same day.
3. There should be at least {MIN_DAYS_BETWEEN_EXAMS} day(s) between exams for any student.
4. Use the provided time slots: {EXAM_TIME_SLOTS}.
5. It is acceptable to schedule multiple different course exams (up to a maximum of 5) on the same day, provided rule #2 (no student conflicts) is strictly met.
6. Try to schedule exams so that courses with many students in common are not scheduled close to each other (lower priority than other rules).
7. If skipping weekends (configured as {SCHEDULING['skip_weekends']}), do not schedule exams on Saturdays or Sundays.

Respond with ONLY a JSON object containing:
1. A "schedule" array: [ {{"course_code": "...", "date": "YYYY-MM-DD", "time_slot": "..."}}, ... ]
2. A "statistics" object with scheduling metrics.
3. An "issues" array listing any problems or constraints that couldn't be fully met.

Do not include any introductory text, markdown formatting (like ```json), or explanations outside the JSON structure itself.
        """}
    ]
    
    payload = {
        "model": DEEPSEEK_API['model'],
        "messages": messages,
        "temperature": DEEPSEEK_API['temperature'],
        "max_tokens": DEEPSEEK_API['max_tokens']
    }
    
    try:
        print("Calling DeepSeek API...")
        response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=120) # Add timeout
        response.raise_for_status()
        
        # Extract and parse the JSON response
        result = response.json()
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "{}")
        
        # --- FIX: Clean the content before parsing ---
        # Remove potential markdown fences and surrounding whitespace/newlines
        cleaned_content = re.sub(r"^\s*```[a-zA-Z]*\s*|\s*```\s*$", "", content).strip()

        try:
            schedule_data = json.loads(cleaned_content)
            print("API call successful and JSON parsed.")
            return schedule_data
        except json.JSONDecodeError as json_err:
            # Provide more context on JSON parsing failure
            print(f"Error: API response could not be parsed as JSON: {json_err}")
            print("--- Raw Response Content ---")
            print(content)
            print("--- Cleaned Response Content ---")
            print(cleaned_content)
            print("-----------------------------")
            print("Falling back to local generation method")
            return generate_schedule_locally(api_data) # Fallback on JSON error
        
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        print("Falling back to local generation method")
        return generate_schedule_locally(api_data)

def generate_schedule_locally(api_data):
    """Generate a schedule locally within the specific date range, allowing up to 5 exams per day."""
    print("Generating schedule using local algorithm within specific date range (max 5 exams/day)." )
    
    # Extract courses and students from API data
    all_courses = []
    for dept_level, courses in api_data['courses_by_dept_level'].items():
        all_courses.extend([c['code'] for c in courses])
    
    students = api_data['students']
    
    # Get conflict analysis for high-conflict courses
    conflict_pairs = {}
    for conflict in api_data['conflict_analysis']:
        course1 = conflict['course1']
        course2 = conflict['course2']
        count = conflict['common_students']
        
        if course1 not in conflict_pairs:
            conflict_pairs[course1] = {}
        conflict_pairs[course1][course2] = count
        
        if course2 not in conflict_pairs:
            conflict_pairs[course2] = {}
        conflict_pairs[course2][course1] = count
    
    # Sort courses by most conflicts first
    course_conflict_count = {}
    for course in all_courses:
        conflict_count = sum(conflict_pairs.get(course, {}).values())
        course_conflict_count[course] = conflict_count
    
    sorted_courses = sorted(all_courses, key=lambda x: course_conflict_count.get(x, 0), reverse=True)
    
    # Map students to their courses
    student_courses = {}
    for student in students:
        student_courses[student['id']] = student['courses']
    
    # --- Use specific date range ---
    start_date = SPECIFIC_START_DATE
    end_date = SPECIFIC_END_DATE
    print(f"Local scheduling attempting between {start_date} and {end_date}")

    # Initialize schedule
    schedule = []
    course_dates = {} # Stores {course_code: (date_obj, time_slot)} for lookup
    student_exam_dates = {} # Stores {student_id: [(date_obj, time_slot), ...]}
    # --- FIX: Add counter for exams per day ---
    exams_per_day = defaultdict(int) # Count courses scheduled per date
    unscheduled_courses = []

    # --- Iterate through dates in the specific range ---
    available_datetimes = []
    current_loop_date = start_date
    while current_loop_date <= end_date:
        # Skip weekends if configured
        if SCHEDULING['skip_weekends'] and current_loop_date.weekday() >= 5:
            current_loop_date += timedelta(days=1)
            continue
        for time_slot in EXAM_TIME_SLOTS:
            available_datetimes.append((current_loop_date, time_slot))
        current_loop_date += timedelta(days=1)

    if not available_datetimes:
        print("Error: No available datetime slots in the specified range.")
        return None

    print(f"Generated {len(available_datetimes)} available time slots for local scheduling.")

    # Schedule each course
    for course in sorted_courses:
        suitable_datetime_found = False

        # Iterate through available slots within the date range
        # Make a copy to iterate over if we modify the original list later (though we aren't here)
        slots_to_try = list(available_datetimes) 
        random.shuffle(slots_to_try) # Randomize slot order to avoid packing early days

        for test_date, time_slot in slots_to_try:
            conflicts = False

            # --- FIX: Check day limit FIRST ---
            if exams_per_day[test_date] >= 5:
                # conflicts = True # Don't set conflict flag, just skip this slot for this course
                continue # This day is full, try the next available slot

            # Find students taking this course
            course_students = [s_id for s_id, courses in student_courses.items() if course in courses]

            # Check for student-specific conflicts with this date and time
            for student_id in course_students:
                if student_id in student_exam_dates:
                    for other_date, other_time in student_exam_dates[student_id]:
                        # Same day conflict check
                        if test_date == other_date and SCHEDULING['no_same_day_exams']:
                            conflicts = True
                            break # Conflict for this student on this day

                        # Min days between check
                        days_diff = abs((test_date - other_date).days)
                        # Check if the difference is positive but less than required minimum
                        if 0 < days_diff < MIN_DAYS_BETWEEN_EXAMS:
                             conflicts = True
                             break # Conflict for this student (too close)
                if conflicts:
                    break # Move to check next slot for this course

            if not conflicts:
                # We found a suitable date and time
                date_str = test_date.strftime("%Y-%m-%d")

                # Record this exam for the course
                course_dates[course] = (test_date, time_slot)
                # --- FIX: Increment day count ---
                exams_per_day[test_date] += 1

                # Record this exam for each student taking the course
                for student_id in course_students:
                    if student_id not in student_exam_dates:
                        student_exam_dates[student_id] = []
                    student_exam_dates[student_id].append((test_date, time_slot))

                # Add to schedule
                schedule.append({
                    "course_code": course,
                    "date": date_str,
                    "time_slot": time_slot
                })

                suitable_datetime_found = True

                break # Slot found for this course, move to the next course

        if not suitable_datetime_found:
            unscheduled_courses.append(course)
            print(f"Warning: Could not find a suitable slot for course {course} within the specified date range and constraints.")

    # --- FIX: Analyze schedule based on the potentially limited schedule ---
    scheduled_dates = [datetime.strptime(s['date'], "%Y-%m-%d").date() for s in schedule]
    if not scheduled_dates:
         print("Warning: No courses were scheduled.")
         total_days = 0
    else:
         actual_start_date = min(scheduled_dates)
         actual_end_date = max(scheduled_dates)
         total_days = (actual_end_date - actual_start_date).days + 1


    # Find conflicts (mainly for reporting if constraints were violated - shouldn't happen with this logic if courses scheduled)
    # ... (existing conflict checking logic) ...
    conflicts = [] # Recalculate based on actual student_exam_dates
    for student_id, exams in student_exam_dates.items():
        # Sort exams by date object
        sorted_exams = sorted(exams, key=lambda x: x[0])

        # Check for same-day conflicts
        for i in range(len(sorted_exams) - 1):
            date1, time1 = sorted_exams[i]
            date2, time2 = sorted_exams[i+1]
            if date1 == date2 and SCHEDULING['no_same_day_exams']:
                # Find course codes corresponding to these times for reporting
                course1_code = next((s['course_code'] for s in schedule if datetime.strptime(s['date'], "%Y-%m-%d").date() == date1 and s['time_slot'] == time1), None)
                course2_code = next((s['course_code'] for s in schedule if datetime.strptime(s['date'], "%Y-%m-%d").date() == date2 and s['time_slot'] == time2), None)
                if course1_code and course2_code:
                    conflicts.append({
                        "type": "Same Day Conflict",
                        "student_id": student_id,
                        "date": date1.strftime("%Y-%m-%d"),
                        "exams": [course1_code, course2_code]
                    })

        # Check for insufficient days between exams
        for i in range(len(sorted_exams) - 1):
            date1, time1 = sorted_exams[i]
            date2, time2 = sorted_exams[i+1]
            days_diff = (date2 - date1).days

            if 0 <= days_diff < MIN_DAYS_BETWEEN_EXAMS: # Should catch same day too if min_days >= 1
                 # Find course codes corresponding to these times for reporting
                course1_code = next((s['course_code'] for s in schedule if datetime.strptime(s['date'], "%Y-%m-%d").date() == date1 and s['time_slot'] == time1), None)
                course2_code = next((s['course_code'] for s in schedule if datetime.strptime(s['date'], "%Y-%m-%d").date() == date2 and s['time_slot'] == time2), None)
                if course1_code and course2_code:
                    conflicts.append({
                        "type": "Insufficient Days Conflict",
                        "student_id": student_id,
                        "date1": date1.strftime("%Y-%m-%d"),
                        "date2": date2.strftime("%Y-%m-%d"),
                        "days_between": days_diff,
                        "exams": [course1_code, course2_code]
                    })

    # Create the response object
    response = {
        "schedule": schedule,
        "statistics": {
            "total_courses_requested": len(all_courses),
            "total_courses_scheduled": len(schedule),
            "total_scheduling_days_used": total_days,
            "total_students": len(students),
            "unresolved_student_conflicts": len(conflicts), # Conflicts found *after* scheduling attempt
            "courses_not_scheduled": len(unscheduled_courses)
        },
        "conflicts": conflicts, # Report conflicts found in the generated schedule
        "issues": [f"Course {c} could not be scheduled within the constraints." for c in unscheduled_courses]
    }

    return response

def save_schedule_to_csv(schedule_data, output_file):
    """Save the generated schedule to a CSV file"""
    if not schedule_data or 'schedule' not in schedule_data:
        print("No valid schedule data to save")
        return False
    
    try:
        # Sort schedule by date and time
        sorted_schedule = sorted(
            schedule_data['schedule'], 
            key=lambda x: (x['date'], x['time_slot'])
        )
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['date', 'time_slot', 'course_code', 'note']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for item in sorted_schedule:
                # Ensure all fields exist
                if 'note' not in item:
                    item['note'] = ''
                writer.writerow({
                    'date': item['date'],
                    'time_slot': item['time_slot'],
                    'course_code': item['course_code'],
                    'note': item.get('note', '')
                })
        
        print(f"Schedule saved to {output_file}")
        return True
    except Exception as e:
        print(f"Error saving schedule to CSV: {e}")
        return False

def save_schedule_to_database(connection, schedule_data):
    """Save the generated schedule to the database"""
    if not schedule_data or 'schedule' not in schedule_data:
        print("No valid schedule data to save")
        return False
    
    cursor = connection.cursor()
    
    try:
        # Check if exam_schedule table exists, if not create it
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS exam_dates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            course_code VARCHAR(10) NOT NULL,
            exam_date DATE NOT NULL,
            time_slot VARCHAR(20) NOT NULL,
            note TEXT,
            UNIQUE KEY (course_code)
        )
        """)
        
        # Insert or update schedule data
        for item in schedule_data['schedule']:
            course_code = item['course_code']
            exam_date = item['date']
            time_slot = item['time_slot']
            note = item.get('note', '')
            
            cursor.execute("""
            INSERT INTO exam_dates (course_code, exam_date, time_slot, note)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            exam_date = VALUES(exam_date),
            time_slot = VALUES(time_slot),
            note = VALUES(note)
            """, (course_code, exam_date, time_slot, note))
        
        connection.commit()
        print(f"Schedule saved to database with {cursor.rowcount} courses updated")
        return True
    except Exception as e:
        print(f"Error saving schedule to database: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate an exam schedule')
    parser.add_argument('--output', default=OUTPUT['default_csv_filename'], help='Output CSV file name')
    parser.add_argument('--save-to-db', action='store_true', help='Save schedule to database')
    parser.add_argument('--api-key', help='DeepSeek API key')
    parser.add_argument('--config', help='Path to config file (overrides default config)')
    
    args = parser.parse_args()
    
    # Set API key if provided via command line (overrides config file)
    global DEEPSEEK_API_KEY
    if args.api_key:
        DEEPSEEK_API_KEY = args.api_key
        print(f"Using API key from command line")
    elif DEEPSEEK_API['key']:
        DEEPSEEK_API_KEY = DEEPSEEK_API['key']
        print(f"Using API key from config file")
    else:
        print(f"No API key provided. Will use local scheduling algorithm.")
    
    # Override default save-to-db setting if specified in command line
    save_to_db = args.save_to_db or OUTPUT['save_to_database']
    
    try:
        # Connect to database
        connection = get_db_connection()
        if not connection:
            return
        
        # Fetch data
        enrollments = fetch_student_enrollments(connection)
        courses = fetch_course_info(connection)
        
        if not enrollments or not courses:
            print("Error: Failed to fetch required data from database")
            return
        
        # Group enrollments by student
        students = group_enrollments_by_student(enrollments)
        print(f"Processed {len(students)} students")
        
        # Prepare data for API
        api_data = prepare_data_for_api(students, courses, enrollments)
        
        # Generate schedule using DeepSeek API or local fallback
        print("\nGenerating schedule...")
        schedule_data = call_deepseek_api(api_data)
        
        if not schedule_data:
            print("Failed to generate schedule")
            return
        
        # Output schedule statistics
        stats = schedule_data.get('statistics', {})
        print("\nSchedule Generation Complete:")
        print(f"Total courses: {stats.get('total_courses_requested', 'N/A')}")
        print(f"Total days: {stats.get('total_scheduling_days_used', 'N/A')}")
        print(f"Conflicts: {stats.get('unresolved_student_conflicts', 'N/A')}")
        
        # Save the schedule to CSV
        save_schedule_to_csv(schedule_data, args.output)
        
        # Save to database if requested
        if save_to_db:
            save_schedule_to_database(connection, schedule_data)
        
        connection.close()
        
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main() 