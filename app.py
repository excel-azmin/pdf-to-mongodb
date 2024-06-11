import fitz  # PyMuPDF
from pymongo import MongoClient
import os

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    document = fitz.open(pdf_path)
    text = ""
    for page_num in range(len(document)):
        page = document.load_page(page_num)
        text += page.get_text()
    return text

def clean_and_structure_text(text):
    """Clean and structure text data. Customize this function based on your PDF structure."""
    # Example: Split text into paragraphs
    paragraphs = text.split('\n\n')
    structured_data = [{"paragraph": para} for para in paragraphs if para.strip()]
    return structured_data

def load_data_to_mongodb(data, db_name, collection_name, mongo_uri="mongodb://68.183.81.8:27011/mongoose_db"):
    """Load data into MongoDB."""
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    result = collection.insert_many(data)
    return result.inserted_ids

def process_pdf_files_in_directory(directory, db_name, collection_name):
    for filename in os.listdir(directory):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(directory, filename)
            print(f"Processing file: {pdf_path}")
            
            # Step 1: Extract text from PDF
            extracted_text = extract_text_from_pdf(pdf_path)
            
            # Step 2: Clean and structure the extracted text
            structured_data = clean_and_structure_text(extracted_text)
            
            # Step 3: Load structured data into MongoDB
            inserted_ids = load_data_to_mongodb(structured_data, db_name, collection_name)
            
            print(f"Data from {filename} successfully inserted into MongoDB. IDs: {inserted_ids}")

if __name__ == "__main__":
    directory = "/home/azmin/Documents/data-load-into-mongodb/data/"  
    db_name = "ai-model-data"
    collection_name = "your_collection_name"
    process_pdf_files_in_directory(directory, db_name, collection_name)
