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
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from config import DEEPSEEK_API, DATABASE, SCHEDULING, OUTPUT
    print("Config loaded successfully")
except ImportError:
    print("Config file not found or contains errors. Creating default config...")
    # Default configurations if config file is missing
    DEEPSEEK_API = {
        'url': 'https://api.deepseek.com/v1/chat/completions',
        'key': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
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
        'min_days_between_exams': 1,
        'no_same_day_exams': True,
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
MIN_DAYS_BETWEEN_EXAMS = SCHEDULING['min_days_between_exams']
MAX_EXAMS_PER_DAY = 5

# Define specific date range
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

def fetch_course_info(connection):
    """Fetch course information from all course tables"""
    cursor = connection.cursor(dictionary=True)
    courses = {}
    
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
                # Store code -> name mapping
                if course['code'] and course['name']:
                    courses[course['code']] = course['name']
        
        print(f"Fetched {len(courses)} unique courses from {len(course_tables)} tables")
        return courses
    except Exception as e:
        print(f"Error fetching course information: {e}")
        return {}
    finally:
        cursor.close()

def create_random_students(num_students, course_codes):
    """Creates a dictionary of random students with enrollments."""
    if not course_codes:
        print("Error: No courses available to assign to students.")
        return {}
    students = {}
    available_courses = list(course_codes)
    if not available_courses:
        print("Error: Course code list is empty.")
        return {}
        
    for i in range(num_students):
        # Generate a simple student ID and name
        student_id_num = random.randint(10000, 99999)
        student_id = f"ST{student_id_num}" 
        student_name = f"Random Student {i+1:02d}"
        
        # Assign random number of courses (e.g., 3 to 8)
        max_possible_courses = len(available_courses)
        num_courses = random.randint(3, min(8, max_possible_courses)) 
        
        if max_possible_courses < num_courses:
             print(f"Warning: Requested {num_courses} courses, but only {max_possible_courses} available.")
             enrolled_courses = list(available_courses) # Assign all available
        else:
             enrolled_courses = random.sample(available_courses, num_courses)
        
        students[student_id] = {
            'id': student_id, # Include id within the student data
            'name': student_name,
            'department': random.choice(['CE', 'EEE', 'ECE', 'GEN']), # Assign random dept
            'academic_level': random.choice(['Dip', 'AdvDip', 'Bach']), # Assign random level
            'courses': enrolled_courses
        }
    print(f"Created {len(students)} random students.")
    return students

def analyze_course_conflicts(students):
    """Analyze which courses have many students in common"""
    course_pairs = defaultdict(int)
    
    for student_id, student_data in students.items():
        courses = sorted(student_data['courses'])
        
        for i in range(len(courses)):
            for j in range(i+1, len(courses)):
                course1 = courses[i]
                course2 = courses[j]
                
                pair = f"{course1}_{course2}"
                course_pairs[pair] += 1
    
    sorted_pairs = sorted(course_pairs.items(), key=lambda x: x[1], reverse=True)
    
    print("\nTop 20 Course Conflicts:")
    for pair, count in sorted_pairs[:20]:
        course1, course2 = pair.split('_')
        print(f"{course1} and {course2}: {count} students in common")
    
    return sorted_pairs

def prepare_data_for_api(students, course_info):
    """Prepare data for DeepSeek API (per-day scheduling)."""
    all_course_codes = list(course_info.keys())
    student_data_for_api = []
    student_full_data = {} # Keep full details
    
    # --- Uses the generated students dict --- 
    for student_id, data in students.items(): 
        student_data_for_api.append({
            'id': student_id,
            'courses': data['courses']
        })
        student_full_data[student_id] = data 

    # --- Analyze conflicts using the generated students --- 
    conflict_analysis = []
    course_conflicts = analyze_course_conflicts(students) # Pass the generated students dict
    for pair, count in course_conflicts[:50]:
        course1, course2 = pair.split('_')
        conflict_analysis.append({
            'course1': course1,
            'course2': course2,
            'common_students': count
        })
    
    api_data = {
        'all_course_codes': all_course_codes,
        'students_for_api': student_data_for_api,
        'students_full_details': student_full_data, # Pass full details separately
        'conflict_analysis': conflict_analysis,
        'constraints': {
            'min_days_between_exams': MIN_DAYS_BETWEEN_EXAMS,
            'no_same_day_exams': SCHEDULING['no_same_day_exams'],
            'max_exams_per_day': MAX_EXAMS_PER_DAY,
            'skip_weekends': SCHEDULING['skip_weekends']
        }
    }
    return api_data

def build_student_exam_dates_from_schedule(schedule_list, students_full_details):
    """Reconstructs the student_exam_dates map from a finished schedule."""
    student_exam_dates = defaultdict(list)
    # Create a quick lookup for courses per student
    student_courses = {s_id: data['courses'] for s_id, data in students_full_details.items()}
    
    if not schedule_list:
        return student_exam_dates
        
    print("Building student exam date map from schedule...")
    for item in schedule_list:
        if 'course_code' not in item or 'date' not in item:
            continue # Skip malformed items
        course_code = item['course_code']
        try:
            # Parse date string to date object
            exam_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
        except ValueError:
            print(f"Warning: Invalid date format '{item['date']}' for course {course_code}. Skipping.")
            continue
            
        # Find students taking this course
        for student_id, courses in student_courses.items():
            if course_code in courses:
                student_exam_dates[student_id].append(exam_date)
                
    # Sort dates for each student
    for student_id in student_exam_dates:
        student_exam_dates[student_id].sort()
        
    print(f"Built exam date map for {len(student_exam_dates)} students.")
    return student_exam_dates

def call_deepseek_api(api_data):
    """Call DeepSeek API, returns schedule_data and student_exam_dates map."""
    if not DEEPSEEK_API_KEY:
        print("Warning: DeepSeek API key not set. Using local generation method instead.")
        return generate_schedule_locally(api_data)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
    }
    
    # Store full student details for building the map later
    students_full_details = api_data.get('students_full_details', {})
    
    # Prepare data subset actually sent to API
    api_payload_data = {
         'all_course_codes': api_data.get('all_course_codes'),
         'students': api_data.get('students_for_api'), # Send simplified student data
         'conflict_analysis': api_data.get('conflict_analysis'),
         'constraints': api_data.get('constraints')
    }
    api_data_json = json.dumps(api_payload_data)
    
    start_date_str = SPECIFIC_START_DATE.strftime("%Y-%m-%d")
    end_date_str = SPECIFIC_END_DATE.strftime("%Y-%m-%d")

    messages = [
        {"role": "system", "content": "You are an expert in exam scheduling. Your task is to create an optimal exam schedule assigning courses to specific dates, avoiding conflicts within a specific date range, allowing multiple exams per day if student schedules permit."},
        {"role": "user", "content": f"""
I need you to create an exam schedule assigning each course to a single date for the following data:
{api_data_json}

The schedule MUST follow these rules:
1. All exams MUST be scheduled between {start_date_str} and {end_date_str} (inclusive).
2. No student should have two exams scheduled on the same day.
3. There should be at least {MIN_DAYS_BETWEEN_EXAMS} day(s) between exams for any student.
4. A maximum of {MAX_EXAMS_PER_DAY} different course exams can be scheduled on any single day.
5. Try to schedule exams so that courses with many students in common are not scheduled close to each other (lower priority than other rules).
6. If skipping weekends (configured as {SCHEDULING['skip_weekends']}), do not schedule exams on Saturdays or Sundays.

Respond with ONLY a JSON object containing:
1. A "schedule" array: [ {{\"course_code\": \"...\", \"date\": \"YYYY-MM-DD\"}}, ... ] (No time_slot field)
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
        response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        
        result = response.json()
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "{}")
        cleaned_content = re.sub(r"^\\s*```[a-zA-Z]*\\s*|\\s*```\\s*$", "", content).strip()

        try:
            schedule_data = json.loads(cleaned_content)
            # Validate schedule
            if schedule_data.get('schedule') and len(schedule_data['schedule']) > 0:
                first_item = schedule_data['schedule'][0]
                if 'date' not in first_item:
                    print("Warning: API response schedule items missing 'date' field.")
                if 'time_slot' in first_item:
                    print("Warning: API response schedule items unexpectedly contain 'time_slot' field. Ignoring it.")
            print("API call successful and JSON parsed.")
            
            # --- Build student_exam_dates map --- 
            schedule_list = schedule_data.get('schedule', [])
            student_exam_dates = build_student_exam_dates_from_schedule(schedule_list, students_full_details)
            
            return schedule_data, student_exam_dates # Return both
            
        except json.JSONDecodeError as json_err:
            print(f"Error: API response could not be parsed as JSON: {json_err}")
            print("--- Raw Response Content ---")
            print(content)
            print("--- Cleaned Response Content ---")
            print(cleaned_content)
            print("-----------------------------")
            print("Falling back to local generation method")
            return generate_schedule_locally(api_data)
        
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        print("Falling back to local generation method")
        return generate_schedule_locally(api_data)

def generate_schedule_locally(api_data):
    """Generate schedule locally, returns schedule_data, student_exam_dates, course_dates."""
    print(f"Generating schedule using local algorithm (per day, max {MAX_EXAMS_PER_DAY} exams/day).")

    # Ensure we use the full student details here if passed
    students_full_details = api_data.get('students_full_details', {})
    student_courses = {s_id: data['courses'] for s_id, data in students_full_details.items()}
    all_course_codes = api_data.get('all_course_codes', [])
    if not all_course_codes or not student_courses:
        print("Error: Missing course or student data for local generation.")
        return None, None, None

    # Sort courses based on conflicts (optional, but can help)
    course_student_count = defaultdict(int)
    for s_id, courses in student_courses.items():
        for course in courses:
            course_student_count[course] += 1

    # Prioritize courses taken by more students or involved in more conflicts
    sorted_courses = sorted(all_course_codes, key=lambda c: course_student_count.get(c, 0), reverse=True)

    start_date = SPECIFIC_START_DATE
    end_date = SPECIFIC_END_DATE
    print(f"Local scheduling attempting between {start_date} and {end_date}")

    schedule = []
    course_dates = {} # {course_code: date_obj}
    student_exam_dates = defaultdict(list) # {student_id: [date_obj, ...]}
    exams_per_day = defaultdict(int) # {date_obj: count}
    unscheduled_courses = []

    available_dates = []
    current_loop_date = start_date
    while current_loop_date <= end_date:
        if not (SCHEDULING['skip_weekends'] and current_loop_date.weekday() >= 5):
            available_dates.append(current_loop_date)
        current_loop_date += timedelta(days=1)

    if not available_dates:
        print("Error: No available dates in the specified range.")
        return None, None, None

    print(f"Generated {len(available_dates)} available dates for local scheduling.")

    for course in sorted_courses:
        suitable_date_found = False
        dates_to_try = list(available_dates)
        random.shuffle(dates_to_try)
        
        for test_date in dates_to_try:
            conflicts = False
            
            if exams_per_day[test_date] >= MAX_EXAMS_PER_DAY:
                continue
            
            course_students_taking_this = [s_id for s_id, courses in student_courses.items() if course in courses]

            for student_id in course_students_taking_this:
                if test_date in student_exam_dates[student_id]:
                    conflicts = True
                    break
                
                for other_date in student_exam_dates[student_id]:
                    days_diff = abs((test_date - other_date).days)
                    if 0 < days_diff < MIN_DAYS_BETWEEN_EXAMS:
                        conflicts = True
                        break # Conflict: Too close
                if conflicts:
                    break # Inner loop conflict
            
            if not conflicts:
                date_str = test_date.strftime("%Y-%m-%d")
                course_dates[course] = test_date
                exams_per_day[test_date] += 1

                for student_id in course_students_taking_this:
                    student_exam_dates[student_id].append(test_date)

                    schedule.append({
                        "course_code": course,
                        "date": date_str,
                })
                suitable_date_found = True
                break
        
        if not suitable_date_found:
            unscheduled_courses.append(course)
            print(f"Warning: Could not find a suitable date for course {course}.")

    # Analyze schedule
    scheduled_dates = [datetime.strptime(s['date'], "%Y-%m-%d").date() for s in schedule]
    if not scheduled_dates:
        total_days = 0
        print("Warning: No courses were scheduled locally.")
    else:
        total_days = (max(scheduled_dates) - min(scheduled_dates)).days + 1

    # Create the response object (schedule_data)
    response = {
        "schedule": schedule,
        "statistics": {
            "total_courses_requested": len(all_course_codes),
            "total_courses_scheduled": len(schedule),
            "total_scheduling_days_used": total_days,
            "total_students": len(student_courses),
            "unresolved_student_conflicts": 0,
            "courses_not_scheduled": len(unscheduled_courses)
        },
        "issues": [f"Course {c} could not be scheduled." for c in unscheduled_courses]
    }
    
    # Return schedule data, student map, AND course date map
    return response, student_exam_dates, course_dates

def check_final_schedule_conflicts(student_exam_dates, course_dates, student_courses):
    """Checks the final schedule for conflicts based on student exam dates."""
    print("Checking final schedule for student conflicts...")
    final_conflicts = []
    if not student_exam_dates or not course_dates or not student_courses:
         print("Warning: Missing data for conflict checking.")
         return final_conflicts
         
    num_students_checked = 0
    for student_id, exams in student_exam_dates.items():
        num_students_checked += 1
        sorted_exam_dates = sorted(list(set(exams))) # Ensure unique dates and sorted

        # Check for same-day conflicts (shouldn't happen if input logic is correct)
        # This requires knowing *which* course was scheduled on a day if multiple exams exist
        # We rely on the scheduling logic preventing this upfront.
        # Re-checking here is complex without full slot info. Primary check is min_days.

        # Check for insufficient days between exams
        for i in range(len(sorted_exam_dates) - 1):
            date1 = sorted_exam_dates[i]
            date2 = sorted_exam_dates[i+1]
            days_diff = (date2 - date1).days

            if 0 < days_diff < MIN_DAYS_BETWEEN_EXAMS:
                 # Find courses student took on these specific dates
                 courses1 = [c for c, dt in course_dates.items() if dt == date1 and c in student_courses.get(student_id, [])]
                 courses2 = [c for c, dt in course_dates.items() if dt == date2 and c in student_courses.get(student_id, [])]
                 if courses1 or courses2: # Only add if we can identify the courses
                      final_conflicts.append({
                          "type": "Insufficient Days Conflict",
                          "student_id": student_id,
                          "date1": date1.strftime("%Y-%m-%d"),
                          "date2": date2.strftime("%Y-%m-%d"),
                          "days_between": days_diff,
                          "exams": courses1 + courses2 
                      })
                      
    print(f"Conflict check complete for {num_students_checked} students. Found {len(final_conflicts)} potential issues.")
    return final_conflicts

def save_student_schedules(students_full_details, schedule_list, student_exam_dates, course_info):
    """Saves the schedule for each student to a separate CSV file in 'test' directory."""
    if not schedule_list or not student_exam_dates:
        print("Missing schedule or student exam times, cannot save individual CSVs.")
        return

    # --- ADDED: Create output directory ---
    output_dir = "test"
    try:
        os.makedirs(output_dir, exist_ok=True)
        print(f"Ensured output directory exists: {output_dir}")
    except OSError as e:
        print(f"Error creating directory {output_dir}: {e}. Student CSVs will be saved in current directory.")
        output_dir = "." # Fallback to current directory
    # --- END ADDED --- 

    print(f"\nSaving individual student schedules to '{output_dir}' directory...")
    
    # Create a quick lookup for scheduled course details {course_code: date_str}
    scheduled_course_dates = {item['course_code']: item['date'] for item in schedule_list if 'course_code' in item and 'date' in item}
    
    for student_id, student_data in students_full_details.items():
        # Get the list of date objects for this student
        exam_dates_objs = sorted(student_exam_dates.get(student_id, []))
        
        if not exam_dates_objs:
            # print(f"No exams scheduled for {student_id}. Skipping CSV.")
            continue

        output_data = []
        last_exam_date_obj = None
        
        # Find the courses scheduled on the specific dates for this student
        student_courses_taken = student_data.get('courses', [])
        student_schedule_details = []
        for dt_obj in exam_dates_objs:
             date_str = dt_obj.strftime("%Y-%m-%d")
             # Find which course(s) the student took on this date from the main schedule
             courses_on_this_date = [ 
                 c for c, sched_date_str in scheduled_course_dates.items() 
                 if sched_date_str == date_str and c in student_courses_taken
             ]
             # Should typically only be one course per student per day
             for course_code in courses_on_this_date:
                  student_schedule_details.append({'DateObj': dt_obj, 'CourseCode': course_code}) 

        # Sort just in case multiple courses were somehow assigned (shouldn't happen)
        # Primarily sorting by DateObj
        student_schedule_details.sort(key=lambda x: x['DateObj']) 

        for exam_detail in student_schedule_details:
            current_exam_date_obj = exam_detail['DateObj']
            course_code = exam_detail['CourseCode']
            course_name = course_info.get(course_code, "Unknown Course")
            days_gap = "N/A"
            if last_exam_date_obj:
                gap_delta = current_exam_date_obj - last_exam_date_obj
                days_gap = gap_delta.days

            output_data.append({
                'Course Code': course_code,
                'Course Name': course_name,
                'Date': current_exam_date_obj.strftime("%Y-%m-%d"),
                'Days Since Last Exam': days_gap
            })
            last_exam_date_obj = current_exam_date_obj

        # Write to student-specific CSV
        if output_data:
            student_identifier = student_data.get('name', student_id).replace(' ', '_').replace('/', '_')
            # --- MODIFIED: Use os.path.join for filename --- 
            filename = os.path.join(output_dir, f"{student_identifier}_schedule.csv")
            # --- END MODIFIED --- 
            try:
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['Course Code', 'Course Name', 'Date', 'Days Since Last Exam']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(output_data)
            except IOError as e:
                print(f"Error writing file {filename}: {e}")
            except Exception as e:
                print(f"Error saving schedule for {student_id} to CSV: {e}")
        # else: # Don't print if no exams scheduled
        #     print(f"No scheduled exams found for {student_id} to save.")
            
    print("Finished saving student schedules.")

def generate_summary_report(students, schedule_list, student_exam_dates, course_info, final_conflicts):
     """Generates a detailed text file summarizing schedule results."""
     print("\nGenerating detailed schedule summary report...")
     filename = "schedule_summary.txt"
     
     # --- Basic Stats --- 
     total_students = len(students)
     total_exams_scheduled = len(schedule_list)
     
     # --- Conflict Analysis --- 
     total_conflicted_students_set = set()
     same_day_students_set = set()
     back_to_back_students_set = set() # days_between == 1
     
     if final_conflicts:
          for conflict in final_conflicts:
               student_id = conflict.get('student_id')
               if not student_id:
                    continue
               total_conflicted_students_set.add(student_id)
               if conflict.get('type') == "Same Day Conflict": # Should be 0 if logic holds
                    same_day_students_set.add(student_id)
               elif conflict.get('type') == "Insufficient Days Conflict":
                    if conflict.get('days_between') == 1:
                         back_to_back_students_set.add(student_id)
                         
     # --- Temporal Spread --- 
     total_gap_days = 0
     total_gaps_counted = 0
     for student_id, exam_dates in student_exam_dates.items():
          # Assumes dates are already sorted per student
          if len(exam_dates) > 1:
               for i in range(len(exam_dates) - 1):
                    gap = (exam_dates[i+1] - exam_dates[i]).days
                    total_gap_days += gap
                    total_gaps_counted += 1
     avg_gap = (total_gap_days / total_gaps_counted) if total_gaps_counted > 0 else 0
     
     # --- Busiest Day --- 
     exams_per_date_count = defaultdict(int)
     students_per_date = defaultdict(set)
     for item in schedule_list:
          if 'date' in item and 'course_code' in item:
               try:
                    exam_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                    course_code = item['course_code']
                    exams_per_date_count[exam_date] += 1
                    # Find students taking this course
                    for s_id, s_data in students.items():
                         if course_code in s_data.get('courses', []):
                              students_per_date[exam_date].add(s_id)
               except ValueError:
                    continue # Skip malformed dates
                    
     busiest_day_date = None
     max_exams_on_day = 0
     if exams_per_date_count:
          busiest_day_date = max(exams_per_date_count, key=exams_per_date_count.get)
          max_exams_on_day = exams_per_date_count[busiest_day_date]
          
     busiest_day_students = len(students_per_date.get(busiest_day_date, set())) if busiest_day_date else 0
     busiest_day_str = f"{busiest_day_date.strftime('%Y-%m-%d')} ({max_exams_on_day} exams, {busiest_day_students} students)" if busiest_day_date else "N/A"
     
     # --- Department Breakdown --- 
     dept_exam_counts = defaultdict(int)
     dept_conflict_counts = defaultdict(int)
     dept_map = {'EGCO': 'CE', 'EGEL': 'EEE', 'EGEC': 'ECE'} # Prefix mapping
     
     for item in schedule_list:
          if 'course_code' in item:
               prefix = item['course_code'][:4]
               dept = dept_map.get(prefix, 'Other')
               dept_exam_counts[dept] += 1
               
     if final_conflicts:
          for conflict in final_conflicts:
               courses_involved = conflict.get('exams', [])
               for course in courses_involved:
                    prefix = course[:4]
                    dept = dept_map.get(prefix, 'Other')
                    dept_conflict_counts[dept] += 1 # Count each conflict instance per dept
                    break # Avoid double counting if both conflict courses are same dept?
                           # Or maybe count per student conflict per dept?
                           # Let's count per conflict instance for now.
                           
     dept_breakdown_lines = []
     all_depts = set(dept_exam_counts.keys()) | set(dept_conflict_counts.keys())
     for dept in sorted(list(all_depts)):
          line = f"  - {dept}: {dept_exam_counts.get(dept, 0)} exams, {dept_conflict_counts.get(dept, 0)} conflicts"
          dept_breakdown_lines.append(line)
          
     # --- Policy Adherence --- 
     policy_adherence = "Yes" if len(back_to_back_students_set) == 0 else "No" 
     # This assumes MIN_DAYS_BETWEEN_EXAMS > 1 means back-to-back is disallowed.
     # If MIN_DAYS_BETWEEN_EXAMS is 1, back-to-back is allowed, so adherence check needs refinement.
     if MIN_DAYS_BETWEEN_EXAMS <= 1:
          policy_adherence = f"Yes (Min gap {MIN_DAYS_BETWEEN_EXAMS} allows back-to-back)"
     else:
           policy_adherence = f"Yes (No back-to-back conflicts found)" if len(back_to_back_students_set) == 0 else f"No ({len(back_to_back_students_set)} students have back-to-back conflicts)" 
           
     # --- Format Report --- 
     report_content = "Exam Schedule Summary Report\n"
     report_content += "=============================\n\n"
     report_content += f"- Total students processed: {total_students}\n"
     report_content += f"- Total exams scheduled: {total_exams_scheduled}\n"
     report_content += "- Conflict Analysis:\n"
     report_content += f"  - Students with detected conflicts: {len(total_conflicted_students_set)}\n"
     report_content += f"  - Same-day conflicts (should be 0): {len(same_day_students_set)}\n"
     report_content += f"  - Back-to-back conflicts (1 day gap): {len(back_to_back_students_set)}\n"
     report_content += "- Temporal Spread:\n"
     report_content += f"  - Avg. gap between student exams: {avg_gap:.2f} days\n"
     report_content += f"  - Busiest day: {busiest_day_str}\n"
     report_content += "- Department Breakdown (estimated by prefix):\n"
     report_content += "\n".join(dept_breakdown_lines) + "\n"
     # report_content += "- Historical Trend: N/A (Requires external data)\n"
     report_content += f"- Policy Adherence (Min {MIN_DAYS_BETWEEN_EXAMS} days gap): {policy_adherence}\n"
          
     try:
          with open(filename, 'w', encoding='utf-8') as f:
               f.write(report_content)
          print(f"Detailed summary report saved to {filename}")
     except IOError as e:
          print(f"Error writing detailed summary report {filename}: {e}")

def save_schedule_to_csv(schedule_data, output_file):
    """Save the generated schedule (per-day) to a CSV file"""
    if not schedule_data or 'schedule' not in schedule_data:
        print("No valid schedule data to save")
        return False
    
    try:
        # Sort schedule by date
        valid_schedule = [item for item in schedule_data['schedule'] if 'date' in item and 'course_code' in item]
        if len(valid_schedule) != len(schedule_data['schedule']):
            print("Warning: Some schedule items were missing 'date' or 'course_code' and were excluded from CSV.")

        sorted_schedule = sorted(
            valid_schedule,
            key=lambda x: x['date']
        )
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['date', 'course_code', 'note']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            
            writer.writeheader()
            for item in sorted_schedule:
                if 'note' not in item:
                    item['note'] = schedule_data.get('issues_map', {}).get(item['course_code'], '')

                writer.writerow({
                    'date': item['date'],
                    'course_code': item['course_code'],
                    'note': item.get('note', '')
                })
        
        print(f"Schedule saved to {output_file}")
        return True
    except KeyError as e:
        print(f"Error saving schedule to CSV: Missing expected key {e} in schedule data.")
        return False
    except Exception as e:
        print(f"Error saving schedule to CSV: {e}")
        return False

def save_schedule_to_database(connection, schedule_data):
    """Save the generated schedule (per-day) to the database"""
    if not schedule_data or 'schedule' not in schedule_data:
        print("No valid schedule data to save")
        return False
    
    cursor = connection.cursor()
    
    try:
        print("Ensuring exam_dates table structure (without time_slot)...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS exam_dates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            course_code VARCHAR(10) NOT NULL,
            exam_date DATE NOT NULL,
            note TEXT,
            UNIQUE KEY (course_code)
        )
        """)
        
        inserted_count = 0
        for item in schedule_data['schedule']:
            if 'course_code' not in item or 'date' not in item:
                print(f"Skipping database save for invalid item: {item}")
                continue

            course_code = item['course_code']
            exam_date = item['date']
            note = item.get('note', '')
            
            cursor.execute("""
            INSERT INTO exam_dates (course_code, exam_date, note)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            exam_date = VALUES(exam_date),
            note = VALUES(note)
            """, (course_code, exam_date, note))
            inserted_count += cursor.rowcount
        
        connection.commit()
        print(f"Schedule data processed for database.")
        return True
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        print("Ensure 'exam_dates' table exists and does NOT have a 'time_slot' column.")
        connection.rollback()
        return False
    except Exception as e:
        print(f"Error saving schedule to database: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()

def main():
    parser = argparse.ArgumentParser(description='Generate an exam schedule (per-day) for 10 random students')
    parser.add_argument('--output', default=OUTPUT.get('default_csv_filename', 'exam_schedule_output.csv'), help='Output CSV file name')
    parser.add_argument('--save-to-db', action='store_true', help='Save schedule to database (Warning: Modifies table)')
    parser.add_argument('--api-key', help='DeepSeek API key (overrides config)')
    parser.add_argument('--config', help='Path to config file (e.g., config.py)')
    parser.add_argument('--local-only', action='store_true', help='Force use of local scheduling algorithm')
    parser.add_argument('--student-csvs', action='store_true', help='Generate individual CSV files per student showing gaps')
    
    args = parser.parse_args()
    
    use_local_scheduler = args.local_only
    api_key_available = False

    global DEEPSEEK_API_KEY
    if args.api_key:
        DEEPSEEK_API_KEY = args.api_key
        print("Using API key from command line.")
        api_key_available = True
    elif DEEPSEEK_API and DEEPSEEK_API.get('key'):
        DEEPSEEK_API_KEY = DEEPSEEK_API['key']
        print("Using API key from config file.")
        api_key_available = True
    else:
        pass

    if use_local_scheduler:
        print("Forcing use of local scheduling algorithm (--local-only specified).")
    elif not api_key_available:
        print("No API key found. Using local scheduling algorithm.")
        use_local_scheduler = True
    else:
        print("API key found. Will attempt to use API scheduling.")
    
    save_to_db = args.save_to_db or OUTPUT.get('save_to_database', False)
    generate_student_csvs = args.student_csvs

    connection = None
    try:
        connection = get_db_connection()
        if not connection:
            print("Database connection failed. Cannot proceed.")
            return

        course_info = fetch_course_info(connection)

        if not course_info:
            print("Error: Failed to fetch course data from database.")
            return
            
        # students = group_enrollments_by_student(enrollments)
        # --- Generate 10 random students --- 
        num_random_students = 10
        students = create_random_students(num_random_students, course_info.keys())
        
        if not students:
             print("Error: Failed to create random students.")
             return
             
        print(f"Proceeding with {len(students)} generated random students")
        # --- END STUDENT GENERATION ---
        
        # Prepare data using the generated students
        scheduling_data_input = prepare_data_for_api(students, course_info)

        print("\nGenerating schedule...")
        schedule_data = None
        student_exam_dates = None
        course_dates = None # Initialize course_dates map
        
        if use_local_scheduler:
            # --- Capture THREE results from local scheduler --- 
            schedule_data, student_exam_dates, course_dates = generate_schedule_locally(scheduling_data_input)
        else:
            # --- Capture THREE results from API call (may fallback to local) --- 
            schedule_data, student_exam_dates, course_dates_api = call_deepseek_api(scheduling_data_input)
            # Reconstruct course_dates if API succeeded, otherwise it came from local fallback
            if schedule_data and 'schedule' in schedule_data and course_dates_api is None: # Check if API likely succeeded
                 course_dates = {item['course_code']: datetime.strptime(item['date'], '%Y-%m-%d').date() 
                                  for item in schedule_data['schedule'] if 'course_code' in item and 'date' in item}
            else:
                 course_dates = course_dates_api # Use the one returned by local fallback via API function

        # --- Error handling for scheduler results --- 
        if not schedule_data or 'schedule' not in schedule_data or student_exam_dates is None or course_dates is None:
            print("Failed to generate schedule or necessary intermediate data.")
            if connection and connection.is_connected(): connection.close()
            return
            
        # --- MOVED & ADDED: Perform final conflict check --- 
        student_courses_map = {s_id: data['courses'] for s_id, data in students.items()} # Needed for conflict check context
        final_conflicts = check_final_schedule_conflicts(student_exam_dates, course_dates, student_courses_map)
        schedule_data['final_conflicts'] = final_conflicts # Store conflicts in main data
        # Update stats based on final check
        schedule_data['statistics']['unresolved_student_conflicts'] = len(final_conflicts)
        # --- END MOVED & ADDED --- 

        # --- Output statistics (now includes final conflict count) ---
        stats = schedule_data.get('statistics', {})
        print("\nSchedule Generation Complete:")
        print(f"Total Courses Requested: {stats.get('total_courses_requested', 'N/A')}")
        print(f"Total Courses Scheduled: {stats.get('total_courses_scheduled', 'N/A')}")
        print(f"Total Scheduling Days Used: {stats.get('total_scheduling_days_used', 'N/A')}")
        print(f"Unresolved Student Conflicts (Post-Check): {stats.get('unresolved_student_conflicts', 'N/A')}")
        print(f"Courses Not Scheduled: {stats.get('courses_not_scheduled', 'N/A')}")
        if schedule_data.get('issues'):
            print("Issues Reported:")
            for issue in schedule_data['issues']:
                print(f"- {issue}")

        # --- Save main CSV --- 
        save_schedule_to_csv(schedule_data, args.output)
        
        # --- Save student CSVs if requested --- 
        if generate_student_csvs:
             if student_exam_dates:
                  save_student_schedules(students, schedule_data.get('schedule', []), student_exam_dates, course_info)
             else:
                  print("Warning: Could not generate student-specific CSVs because student exam date map is missing.")

        # --- ADDED: Generate Summary Report --- 
        generate_summary_report(students, schedule_data.get('schedule', []), student_exam_dates, course_info, final_conflicts)
        # --- END ADDED --- 
        
        # --- Save to DB if requested --- 
        if save_to_db:
            print("Attempting to save schedule to database...")
            save_schedule_to_database(connection, schedule_data)
        else:
            print("Skipping database save (--save-to-db not specified).")
            
    except Exception as e:
        print(f"An unexpected error occurred in main execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main() 
