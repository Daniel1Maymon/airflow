import re

# Patterns for extraction
UPDATED_PRICE_PATTERN = r'(?<!\d)(?:מחיר[:\s-]*|שכ["׳]?ד[:\s-]*|שכר\s*דירה[:\s-]*|עלות חודשית[:\s-]*)?\s*(\b\d{1,3}(?:,\d{3})+|\b\d{4,})\s*(?:ש["׳]?ח|₪|מיליון|שקל)?(?!\d)'
ROOMS_PATTERN = r'(\d+(\.\d+)?)\s*(?:חדר(?:ים)?|חד)'
SIZE_PATTERN = r'(\d+(\.\d+)?)(?:\s*וחצי)?\s*(?:מ"ר|מטר|מר|מ״ר)\b'

# Extract price
def extract_price(text):
    matches = re.findall(UPDATED_PRICE_PATTERN, text)
    extracted_prices = [int(match.replace(",", "")) for match in matches] if matches else []
    result = extracted_prices[0] if (extracted_prices and len(str(extracted_prices[0])) < 9 )  else None
    return result if extracted_prices else None


# Extract rooms
def extract_rooms(text):
    match = re.search(ROOMS_PATTERN, text)
    return float(match.group(1)) if match else None

# Extract size
def extract_size(text):
    match = re.search(SIZE_PATTERN, text)
    return float(match.group(1)) if match else None

# Main function
def extract_rental_info(text):
    return {
        "price": extract_price(text),
        "rooms": extract_rooms(text),
        "size": extract_size(text),
    }