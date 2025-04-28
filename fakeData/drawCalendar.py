import matplotlib.pyplot as plt
import numpy as np
import calendar
from datetime import datetime, timedelta
import argparse

def draw_calendar(start_date, end_date, output_file="calendar.png"):
    """Draw a simple, clean calendar for the given date range"""
    # Get the start and end months
    start_month = start_date.month
    start_year = start_date.year
    end_month = end_date.month
    end_year = end_date.year
    
    months_to_plot = []
    current_date = datetime(start_year, start_month, 1)
    
    # Generate list of months to plot
    while (current_date.year, current_date.month) <= (end_year, end_month):
        months_to_plot.append((current_date.year, current_date.month))
        # Move to next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)
    
    # Set up the figure with subplots for each month
    num_months = len(months_to_plot)
    cols = min(3, num_months)  # Maximum 3 columns
    rows = (num_months + cols - 1) // cols  # Calculate rows needed
    
    fig, axes = plt.subplots(rows, cols, figsize=(12, 4*rows))
    
    # Flatten axes array for easier iteration if needed
    if rows == 1 and cols == 1:
        axes = np.array([axes])
    elif rows == 1 or cols == 1:
        axes = axes.flatten()
    
    # Create calendar for each month
    for i, (year, month) in enumerate(months_to_plot):
        if i < len(axes) if isinstance(axes, np.ndarray) else 1:
            ax = axes[i] if isinstance(axes, np.ndarray) else axes
            
            # Get the calendar matrix for the month
            cal = calendar.monthcalendar(year, month)
            
            # Set title to month and year without spaces
            month_name = calendar.month_name[month]
            ax.set_title(f"{month_name}{year}", fontweight='bold', fontsize=14)
            
            # Remove axis ticks and labels
            ax.set_xticks([])
            ax.set_yticks([])
            
            # Setup grid
            ax.set_xlim(0, 7)
            ax.set_ylim(0, len(cal) + 1)
            
            # Set the background color to white
            ax.set_facecolor('white')
            
            # Draw day names at the top
            days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            for j, day in enumerate(days):
                ax.text(j+0.5, 0.5, day, ha='center', va='center', 
                       fontweight='bold', fontsize=10, color='navy')
            
            # Draw days
            for week_idx, week in enumerate(cal):
                for day_idx, day in enumerate(week):
                    if day != 0:
                        # Create date object for this day
                        date_obj = datetime(year, month, day)
                        
                        # Determine if date is in range and if it's a weekend
                        in_range = start_date <= date_obj <= end_date
                        is_weekend = day_idx >= 5  # Saturday and Sunday
                        
                        # Set cell color based on date range and weekend
                        if is_weekend:
                            cell_color = '#f0f0f0'  # Light gray for weekends
                        elif in_range:
                            cell_color = '#e6f2ff'  # Light blue for days in range
                        else:
                            cell_color = 'white'
                        
                        # Draw day cell
                        rect = plt.Rectangle((day_idx, len(cal) - week_idx), 1, 1, 
                                            fill=True, color=cell_color, linewidth=1,
                                            edgecolor='#cccccc', alpha=0.7)
                        ax.add_patch(rect)
                        
                        # Draw day number with different color based on range
                        text_color = 'black' if in_range else '#999999'
                        font_weight = 'bold' if in_range else 'normal'
                        ax.text(day_idx+0.5, len(cal)-week_idx+0.5, str(day), 
                               ha='center', va='center', fontsize=10,
                               color=text_color, fontweight=font_weight)
            
            # Draw grid lines
            for x in range(8):
                ax.axvline(x, color='#dddddd', linewidth=0.5)
            for y in range(len(cal) + 2):
                ax.axhline(y, color='#dddddd', linewidth=0.5)
    
    # Hide unused subplots
    if isinstance(axes, np.ndarray):
        for i in range(len(months_to_plot), len(axes)):
            if i < len(axes):
                axes[i].set_visible(False)
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)
    
    # Set the title without spaces
    title_text = f"Calendar:{start_date.strftime('%d%b%Y')}to{end_date.strftime('%d%b%Y')}"
    plt.suptitle(title_text, fontsize=16, y=0.98)
    
    # Save the figure
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Calendar saved as {output_file}")
    
    # Show the figure
    plt.show()

def parse_date(date_str):
    """Parse date string in various formats"""
    formats = ['%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%Y/%m/%d', '%d-%b-%Y', '%d %b %Y']
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    
    raise ValueError(f"Unable to parse date: {date_str}. Please use format DD-MM-YYYY or YYYY-MM-DD.")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate a simple calendar')
    parser.add_argument('--start', required=True, help='Start date (format: DD-MM-YYYY)')
    parser.add_argument('--end', required=True, help='End date (format: DD-MM-YYYY)')
    parser.add_argument('--output', default='calendar.png', help='Output file name')
    
    args = parser.parse_args()
    
    # Parse dates
    try:
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
        
        # Draw calendar
        draw_calendar(start_date, end_date, args.output)
    except ValueError as e:
        print(e)
    except Exception as e:
        print(f"Error: {e}") 