import csv
import random
from datetime import datetime, timedelta

def load_courses_from_csv(filenames):
    """Loads course codes and names from multiple CSV files."""
    courses = {}
    for filename in filenames:
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header_skipped = False
                for row in reader:
                    # Skip comments and empty lines
                    if not row or row[0].strip().startswith('#'):
                        continue
                    # Skip header row if it looks like one
                    if not header_skipped and (row[0].strip().lower() == 'code' or len(row) != 2):
                         header_skipped = True
                         # Handle potential duplicate headers across files
                         if filename != filenames[0] and row[0].strip().lower() == 'code':
                             continue
                         elif filename == filenames[0]:
                             continue


                    if len(row) == 2:
                        code, name = row[0].strip(), row[1].strip()
                        if code and name: # Ensure code and name are not empty
                            courses[code] = name
        except FileNotFoundError:
            print(f"Warning: File {filename} not found. Skipping.")
        except Exception as e:
            print(f"Error reading {filename}: {e}")
    # Correct potential misidentified header if it's a valid course
    if 'Code' in courses and courses['Code'] == 'Name':
        del courses['Code']
    print(f"Loaded {len(courses)} unique courses.")
    return courses

def create_random_students(num_students, course_codes):
    """Creates a list of students with random course enrollments."""
    if not course_codes:
        print("Error: No courses available to assign to students.")
        return {}
    students = {}
    available_courses = list(course_codes)
    for i in range(num_students):
        student_id = f"Student_{i+1:03d}"
        num_courses = random.randint(3, 6)
        # Ensure we don't try to pick more courses than available
        num_courses = min(num_courses, len(available_courses))
        enrolled_courses = random.sample(available_courses, num_courses)
        students[student_id] = enrolled_courses
    print(f"Created {len(students)} students.")
    return students

def generate_simple_schedule(students, course_codes):
    """Generates a simple schedule ensuring no student has clashing exam times.
    Returns the main schedule and student-specific exam times.
    """
    schedule = {} # {course_code: (date_str, time_slot, course_name)}
    # Store date objects here for easier sorting later
    student_exam_times = {student_id: [] for student_id in students} # {student_id: [(date_obj, time_slot, course_code)]}

    # Get unique list of all courses needed across the selected students
    courses_to_schedule = set()
    for courses in students.values():
        courses_to_schedule.update(courses)

    # Define exam parameters
    time_slots = ["8:30-10:30", "11:30-1:30", "2:30-4:30"]
    start_date = datetime.now().date() + timedelta(days=14) # Start in 2 weeks
    current_date = start_date
    slot_index = 0

    print(f"Scheduling {len(courses_to_schedule)} unique courses...")

    for course_code in courses_to_schedule:
        scheduled = False
        attempts = 0
        max_attempts = len(time_slots) * 30 # Try for 30 days

        while not scheduled and attempts < max_attempts:
            attempts += 1
            exam_datetime = (current_date, time_slots[slot_index])
            conflict = False

            # Find students taking this course
            students_taking_course = [s_id for s_id, courses in students.items() if course_code in courses]

            # Check for conflicts for these students
            for student_id in students_taking_course:
                if exam_datetime in student_exam_times[student_id]:
                    conflict = True
                    break # Conflict found for this student, try next slot/day

            if not conflict:
                # Schedule the exam
                course_name = course_codes.get(course_code, "Unknown Course Name")
                date_str = current_date.strftime("%Y-%m-%d")
                schedule[course_code] = (date_str, time_slots[slot_index], course_name)

                # Update student exam times (Store date object and course code)
                for student_id in students_taking_course:
                    # Use current_date (date object) here
                    student_exam_times[student_id].append((current_date, time_slots[slot_index], course_code))

                scheduled = True
                # print(f"Scheduled {course_code} on {schedule[course_code][0]} at {schedule[course_code][1]}")


            # Move to the next slot/day
            slot_index += 1
            if slot_index >= len(time_slots):
                slot_index = 0
                # Simple day increment, doesn't skip weekends
                current_date += timedelta(days=1)

        if not scheduled:
            print(f"Warning: Could not schedule {course_code} without conflicts after {max_attempts} attempts.")
            # Assign anyway with a note? For now, just skip.


    print("Scheduling complete.")
    # Return both the overall schedule and the student-specific times
    return schedule, student_exam_times

def save_student_schedules(students_data, schedule, student_exam_times, course_details):
    """Saves the schedule for each student to a separate CSV file, showing gaps."""
    if not schedule:
        print("No schedule generated to save.")
        return

    print("Saving individual student schedules...")
    for student_id, enrolled_courses in students_data.items():
        student_schedule_info = []

        # Get the scheduled times for this student
        exam_times = student_exam_times.get(student_id, [])

        # Create a list of dictionaries for sorting
        for date_obj, time_slot, course_code in exam_times:
            if course_code in schedule:
                _, _, course_name = schedule[course_code]
                student_schedule_info.append({
                    'Course Code': course_code,
                    'Course Name': course_name,
                    'Date': date_obj, # Keep as object for sorting
                    'Time Slot': time_slot
                })
            else:
                 # This case should ideally not happen if scheduling logic is correct
                 print(f"Warning: Scheduled time found for {course_code} for {student_id}, but course not in main schedule.")

        # Sort the student's exams by date, then time slot
        student_schedule_info.sort(key=lambda x: (x['Date'], x['Time Slot']))

        # Calculate gaps and format for CSV
        output_data = []
        last_exam_date = None
        for exam in student_schedule_info:
            days_gap = "N/A"
            if last_exam_date:
                gap_delta = exam['Date'] - last_exam_date
                days_gap = gap_delta.days

            output_data.append({
                'Course Code': exam['Course Code'],
                'Course Name': exam['Course Name'],
                'Date': exam['Date'].strftime("%Y-%m-%d"), # Format date for CSV
                'Time Slot': exam['Time Slot'],
                'Days Since Last Exam': days_gap
            })
            last_exam_date = exam['Date'] # Update last exam date

        # Write to student-specific CSV
        if output_data:
            filename = f"{student_id}_schedule.csv"
            try:
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['Course Code', 'Course Name', 'Date', 'Time Slot', 'Days Since Last Exam']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(output_data)
                # print(f"Saved schedule for {student_id} to {filename}")
            except Exception as e:
                print(f"Error saving schedule for {student_id} to CSV: {e}")
        else:
            print(f"No scheduled exams found for {student_id} to save.")
    print("Finished saving student schedules.")

# --- Main Execution ---
if __name__ == "__main__":
    # 1. Load Courses
    course_files = ['CE.csv', 'EEE.csv', 'ECE.csv']
    all_courses = load_courses_from_csv(course_files)

    if not all_courses:
        print("Exiting: No course data loaded.")
    else:
        # 2. Create Random Students
        num_students = 10
        students_data = create_random_students(num_students, all_courses.keys())

        if not students_data:
             print("Exiting: No students created.")
        else:
            # 3. Generate Schedule
            # Capture both return values
            final_schedule, student_exam_times = generate_simple_schedule(students_data, all_courses)

            # 4. Save Individual Student Schedules
            # Pass the necessary data to the new function
            save_student_schedules(students_data, final_schedule, student_exam_times, all_courses)

            # Optional: Print student enrollments for reference
            # print("\nStudent Enrollments:")
            # for s_id, courses in students_data.items():
            #     print(f"{s_id}: {', '.join(courses)}") 