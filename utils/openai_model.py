import os
# import openai
import json
from openai import OpenAI
from dotenv import load_dotenv

# Set the OpenAI API key
# openai.api_key = os.environ.get("OPENAI_API_KEY"))
load_dotenv()

client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY")  # This is the default and can be omitted
)


def extract_info(post_text):
    # Define the messages for the conversation

    # Call the OpenAI API
    # response = client.chat.completions.create(
    #     model="gpt-4",
    #     messages=messages
    # )
    
    try:
        messages = [
            {
                "role": "system",
                "content": """
                Extract rental information from text.
                Respond in JSON format only, valid for Python's json.loads.
                
                If the text describes an apartment for rent, return:
                {
                    "rooms": (float),
                    "size": (int),
                    "price": (int or null),
                    "city": (string, Hebrew),
                    "address": (string or null, Hebrew),
                    "phone": (string or null)
                }

                If it does not describe an apartment for rent, return:
                {"result": "False"}.

                Do not include anything else.
                """
            },
            {
                "role": "user",
                "content": f"{post_text}"
            }
        ]
        
        response = client.chat.completions.create(
            model='gpt-4',
            messages=messages
        )

        # Extract the content of the response
        result = response.choices[0].message.content

        # Validate that the result is a valid JSON
        parsed_result = json.loads(result)  # Ensure the result is JSON compatible
        
        return parsed_result

    except json.JSONDecodeError as e:
        print("The response is not valid JSON:", e)
        return {"error": "Invalid JSON response from OpenAI."}

    except Exception as e:
        print("An error occurred:", e)
        return {"error": str(e)}


if __name__ == "__main__":
    # Examples of real estate posts
    examples = [
        """
        היי לכולם
        אני ובן הזוג שלי מחפשים דירה/יחידת דיור
        עד 4200 ש״ח, כניסה בסביבות דצמבר
        עדיפות למרוהטת 
        תודה מראש
        """,
        """
        להשכרה דירת 3 חדרים בשכונת תל גנים המבוקשת בגבעתיים 
        דירה מרווחת, מוארת ונעימה בגודל של כ- 70 מ"ר, עם פרקט ומזגנים בחדרים.
        בקרבת גנים, בתי"ס ומרכזי קניות, עם יציאות מעולות לת"א. 
        כניסה מיידית!
        שכ"ד: 5,200 ש"ח לחודש.
        לפרטים ותיאום ביקור:
        נאוה - 052-3901736 
        """,
        """
        מחפשים דירה משוכרת 7000₪? 
        מחיר לדירה חדשה 2.790.000 ש"ח
        למכירה בבלעדיות בשכונת נגבה דירת 4 חדרים קומה 1 עורפית 84 מטר 
        מרפסת שמש 10 מטר אחרי פינוי בינוי! 
        שוכרים שרוצים להישאר! 
        לפרטים נוספים שלומי מאנה 
        053-439-4187
        המתווך החזק
        """,
        """
        אתה הראש אתה השם 
        תזכורת קטנה 
        לכל מי ששכח לרגע שיש מנהיג לעולם 
        """
    ]

    # Process each example
    for i, example in enumerate(examples):
        print("-----")
        print(f"Example {i+1}: {example.strip()}\n")
        extracted_info = extract_info(example)
        print(f"Extracted Info = \n{extracted_info}")

        # Try to parse the response as JSON
        try:
            extracted_info_in_json = json.loads(extracted_info)
            print(f"Extracted Info in JSON:\n{extracted_info_in_json}\n")
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
