import csv
import random
from datetime import datetime, timedelta
import os

def generate_csv(filename, num_rows, start_id=0):
    """Generate CSV file with sample data"""

    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Toys', 'Sports']
    regions = ['North', 'South', 'East', 'West', 'Central']
    
    # Create directory if it doesn't exist
    directory = os.path.dirname(filename)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'id', 'product_name', 'category', 'price', 
            'quantity', 'region', 'timestamp', 'score'
        ])
        
        # Generate data
        base_time = datetime.now()
        for i in range(num_rows):
            row_id = start_id + i
            writer.writerow([
                row_id,
                f'Product_{row_id}',
                random.choice(categories),
                round(random.uniform(10.0, 999.99), 2),
                random.randint(1, 100),
                random.choice(regions),
                (base_time + timedelta(seconds=i)).isoformat(),
                round(random.uniform(1.0, 5.0), 2)
            ])
    
    print(f"✓ Generated {filename} with {num_rows} rows")

generate_csv('input/input_file_1.csv', 1000, start_id=0)
generate_csv('input/input_file_2.csv', 1000, start_id=1000)