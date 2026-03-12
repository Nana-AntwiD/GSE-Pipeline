import json 
import csv 
import os 
from datetime import date 
from glob import glob 

def get_latest_raw_file(): 
    files = glob("data/raw/gse_live_*.json")
    if not files:
        raise FileNotFoundError("No raw files found in data/raw/")
    return sorted(files)[-1]

def save_to_csv(json_filepath):
    with open(json_filepath) as f:
        data = json.load(f)
        
    today = date.today()
    csv_path = "data/gse_prices.csv"
    file_exists = os.path.exists(csv_path)
    
    with open(csv_path, 'a', newline='') as f:
        fieldnames = ["date", "ticker", "price", "change", "volume"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
            
        for stock in data:
            writer.writerow({
                "date": today,
                "ticker": stock["name"],
                "price": stock["price"],    
                "volume": stock["volume"],
                "change": stock["change"]
            })
            
        print(f"Appended {len(data)} records for {today} -> data/gse_prices.csv")
        
 
 
if __name__ == "__main__":
    filepath = get_latest_raw_file()
    print(f"Processing file: {filepath}")
    save_to_csv(filepath)