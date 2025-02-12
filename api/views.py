from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from pymongo import MongoClient
from django.conf import settings
import boto3
from bson import ObjectId
import traceback
import datetime
from datetime import timedelta
from collections import defaultdict

from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from pymongo import MongoClient
import json
import re
import stripe
import gspread
from supabase import create_client, Client
import firebase_admin
from firebase_admin import credentials, firestore, initialize_app


client = MongoClient(f'{settings.MONGO_URI}')
db = client['Competiboard']
boards_collection = db['Boards']
users_collection = db['Users']
data_collection = db['Data']
aws_access_key_id = settings.AWS_ACCESS_KEY_ID
aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
bucket_name = 'competiboard'


@csrf_exempt
def main(req):
    return HttpResponse("Wsg")


@csrf_exempt
def create_user(request):
    if request.method != "POST":
        return JsonResponse({"message": "Method Not Allowed"}, status=405)
    
    try:
        # Parse the incoming JSON request body
        body = json.loads(request.body)
        data = body.get("data", {})
        id = data.get("id")
        email_addresses = data.get("email_addresses", [])
        first_name = data.get("first_name", "")
        last_name = data.get("last_name", "")
        image_url = data.get("image_url", "")

        # Validate the request data
        if not id or not email_addresses or len(email_addresses) == 0:
            return JsonResponse(
                {"message": "Invalid payload: Missing id or email address"},
                status=400
            )

        # Check if the user already exists
        existing_user = users_collection.find_one({"clerk_id": id})
        if existing_user:
            return JsonResponse(
                {"message": "User already exists", "userId": str(existing_user["_id"])},
                status=200
            )

        # Prepare user data to insert
        user_data = {
            "clerk_id": id,
            "name": f"{first_name} {last_name}".strip(),
            "email": email_addresses[0].get("email_address", ""),
            "created_at": datetime.datetime.today(),
            "profile_picture": image_url,
            "num_boards": 0,
            "plan": "free",
        }

        # Insert the user into the Users collection
        result = users_collection.insert_one(user_data)

        if result.inserted_id:
            return JsonResponse(
                {"message": "User added successfully", "userId": str(result.inserted_id)},
                status=200
            )
        else:
            raise Exception("Failed to insert user")
    
    except Exception as error:
        print("Error adding user:", error)
        return JsonResponse(
            {"message": "Internal Server Error", "error": str(error)},
            status=500
        )


@csrf_exempt
def board_options(req):
    print('recieved')
    try:
        data = json.loads(req.body.decode("utf-8"))
        clerk_id = data.get("clerk_id")

        if not clerk_id:
            print('No clerk_id')
            return JsonResponse({'error': 'clerk_id is required'}, status=400)

        # Query the boards collection for boards associated with the clerk_id
        boards = boards_collection.find({'creator_id': clerk_id})

        # Format the boards
        formatted_boards = [
            {'id': str(board['_id']), 'title': board['title'], 'thumbnail': board['thumbnail'] if "thumbnail" in board else ''}
            for board in boards
        ]

        return JsonResponse({'boards': formatted_boards}, status=200)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({'error': str(e)}, status=500)
    

@csrf_exempt
def add_board(req):
    try:
        print('recieved')
        clerk_id = req.POST.get('clerk_id')
        title = req.POST.get('title', '').lower()
        my_file = req.FILES['file']

        if not clerk_id:
            print('No ClerkID')
            return JsonResponse({'error': 'clerk_id is required'})
        
        user = users_collection.find_one({'clerk_id': clerk_id})
        if not user:
            print('No User')
            return JsonResponse({'error': 'User not found'})
        
        # Check for duplicate title
        existing_board = boards_collection.find_one({'title': title, 'creator_id': clerk_id})
        if existing_board:
            return JsonResponse({'warning': 'A board with this title already exists'}, status=200)

        date = datetime.datetime.today()
        key = f'thumbnails/{clerk_id}_{my_file.name}'

        s3.upload_fileobj(
            my_file,   # Local file path
            bucket_name,    
            key,
            ExtraArgs={'ACL': 'public-read'}
        )

        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{key}"

        data = {
            "creator_id": clerk_id, 
            "creator_name": user['name'],
            "source": "Sheet",
            "api": {},
            "filter_settings": {},
            "date_settings": {},
            "method": {}
        }

        created_data = data_collection.insert_one(data)
        data_id = created_data.inserted_id

        board = {
            "creator_id": clerk_id,
            "creator_name": user['name'],
            'title': title,
            "created_at": date,
            "domain": "",
            "published": False,
            "thumbnail": s3_url, 
            "data": str(data_id),
        }

        created_board = boards_collection.insert_one(board)
        board_id = created_board.inserted_id

        data_collection.update_one(
            {'_id': data_id},
            {'$set': {'board_id': str(board_id)}}
        )

        users_collection.update_one(
            {'clerk_id': clerk_id},
            {'$inc': {'num_boards': 1}}
        )

        return JsonResponse({'success': True}, status=200)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({'error': str(e)}, status=500)
    

@csrf_exempt
def update_data_source(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        source = data.get("source")
        api_data = data.get("data", {})
        
        if not board_id or not clerk_id or not source:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing data document
        existing_data = data_collection.find_one({"board_id": board_id, "creator_id": clerk_id})
        if not existing_data:
            return JsonResponse({"error": "Data document not found"}, status=404)
        
        # Update the document
        update_result = data_collection.update_one(
            {"board_id": board_id, "creator_id": clerk_id},
            {"$set": {"source": source, "api": api_data}}
        )
        
        if update_result.modified_count == 0:
            return JsonResponse({"error": "No changes made"}, status=400)
        
        return JsonResponse({"success": True, "message": "Data updated successfully"}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)
    

@csrf_exempt
def update_data_settings(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        filter_settings = data.get("filter_settings", {})
        date_settings = data.get("date_settings", {})
        expression = data.get("expression", {})
        method = data.get("method")
        
        if not board_id or not clerk_id or not filter_settings or not date_settings or not method:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing data document
        existing_data = data_collection.find_one({"board_id": board_id, "creator_id": clerk_id})
        if not existing_data:
            return JsonResponse({"error": "Data document not found"}, status=404)
        
        # Update the document
        data_collection.update_one(
            {"board_id": board_id, "creator_id": clerk_id},
            {"$set": {"method": method, "filter_settings": filter_settings, "date_settings": date_settings, "expression": expression}}
        )
        
        return JsonResponse({"success": True, "message": "Data updated successfully"}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)
    


@csrf_exempt
def update_display(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        settings = data.get("settings")
        
        if not board_id or not clerk_id or not settings:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing data document
        existing_board = boards_collection.find_one({"_id": ObjectId(board_id), "creator_id": clerk_id})
        if not existing_board:
            return JsonResponse({"error": "Board document not found"}, status=404)
        
        # Update the document
        boards_collection.update_one(
            {"_id": ObjectId(board_id), "creator_id": clerk_id},
            {"$set": {"display": settings}}
        )
        
        return JsonResponse({"success": True, "message": "Data updated successfully"}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)


@csrf_exempt
def data_details(request):
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        
        if not board_id or not clerk_id:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing data document
        existing_data = data_collection.find_one({"board_id": board_id, "creator_id": clerk_id})
        if not existing_data:
            return JsonResponse({"error": "Data document not found"}, status=404)
        
        existing_data['_id'] = str(existing_data['_id'])
        
        return JsonResponse({"data": existing_data}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)
    

@csrf_exempt
def board_details(request):
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        
        if not board_id or not clerk_id:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing data document
        existing_board = boards_collection.find_one({"_id": ObjectId(board_id), "creator_id": clerk_id})
        if not existing_board:
            return JsonResponse({"error": "Board document not found"}, status=404)
        
        existing_board['_id'] = str(existing_board['_id'])
        
        return JsonResponse({"data": existing_board}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)



@csrf_exempt
def public_board_details(request):
    try:
        data = json.loads(request.body)
        board = data.get("board")
        
        if not board:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing data document
        existing_board = boards_collection.find_one({"title": board})
        if not existing_board:
            return JsonResponse({"error": "Board document not found"}, status=404)
        
        existing_board['_id'] = str(existing_board['_id'])
        
        return JsonResponse({"data": existing_board}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)



@csrf_exempt
def generate_leaderboard(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        
        if not board_id or not clerk_id:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        data_settings = data_collection.find_one({"board_id": board_id, "creator_id": clerk_id})
        if not data_settings:
            return JsonResponse({"error": "Data settings not found"}, status=404)

        source = data_settings.get("source")
        api_data = data_settings.get("api", {})
        filter_settings = data_settings.get("filter_settings", {})
        method = data_settings.get("method", "Doc Sum")
        expression = data_settings.get("expression", {})

        # Fetch data based on source
        data = []
        if source == "MongoDB":
            client = MongoClient(api_data.get("uri"))
            db = client[api_data.get("database")]
            collection = db[api_data.get("collection")]
            data = list(collection.find({}))

        elif source == "Supabase":
            supabase: Client = create_client(api_data.get("url"), api_data.get("anonKey"))
            response = supabase.table(api_data.get("table")).select("*").execute()
            data = response.data

        elif source == "Firebase":
            if not firebase_admin._apps:
                cred = credentials.Certificate({
                    "apiKey": api_data.get("apiKey"),
                    "authDomain": api_data.get("authDomain"),
                    "projectId": api_data.get("projectId")
                })
                firebase_admin.initialize_app(cred)
            
            db = firestore.client()
            docs = db.collection(api_data.get("collection")).stream()
            data = [doc.to_dict() for doc in docs]

        elif source == "Sheet":
            gc = gspread.service_account(filename="path_to_google_credentials.json")
            sheet = gc.open_by_url(api_data.get("url"))
            worksheet = sheet.sheet1
            records = worksheet.get_all_records()
            data = [{k: v for k, v in row.items() if v != ''} for row in records]

        else:
            return JsonResponse({"error": "Unsupported data source"}, status=400)

        # Apply filters
        filtered_data = apply_filters(data, filter_settings)

        # Process leaderboard data
        if method == "Doc Sum":
            leaderboard_data = process_doc_sum(filtered_data, expression)
        elif method == "Classic":
            leaderboard_data = process_classic(filtered_data, expression)
        else:
            return JsonResponse({"error": "Unsupported method"}, status=400)

        return JsonResponse({"success": True, "leaderboard": leaderboard_data}, status=200)

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)
    


@csrf_exempt
def public_generate_leaderboard(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    
    try:
        data = json.loads(request.body)
        board = data.get("board")
        
        if not board:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        existing_board = boards_collection.find_one({"title": board})
        if not existing_board:
            return JsonResponse({"error": "Board document not found"}, status=404)
        
        data_settings = data_collection.find_one({"board_id": str(existing_board["_id"])})
        if not data_settings:
            return JsonResponse({"error": "Data settings not found"}, status=404)

        source = data_settings.get("source")
        api_data = data_settings.get("api", {})
        filter_settings = data_settings.get("filter_settings", {})
        method = data_settings.get("method", "Doc Sum")
        expression = data_settings.get("expression", {})

        # Fetch data based on source
        data = []
        if source == "MongoDB":
            client = MongoClient(api_data.get("uri"))
            db = client[api_data.get("database")]
            collection = db[api_data.get("collection")]
            data = list(collection.find({}))

        elif source == "Supabase":
            supabase: Client = create_client(api_data.get("url"), api_data.get("anonKey"))
            response = supabase.table(api_data.get("table")).select("*").execute()
            data = response.data

        elif source == "Firebase":
            if not firebase_admin._apps:
                cred = credentials.Certificate({
                    "apiKey": api_data.get("apiKey"),
                    "authDomain": api_data.get("authDomain"),
                    "projectId": api_data.get("projectId")
                })
                firebase_admin.initialize_app(cred)
            
            db = firestore.client()
            docs = db.collection(api_data.get("collection")).stream()
            data = [doc.to_dict() for doc in docs]

        elif source == "Sheet":
            gc = gspread.service_account(filename="path_to_google_credentials.json")
            sheet = gc.open_by_url(api_data.get("url"))
            worksheet = sheet.sheet1
            records = worksheet.get_all_records()
            data = [{k: v for k, v in row.items() if v != ''} for row in records]

        else:
            return JsonResponse({"error": "Unsupported data source"}, status=400)

        # Apply filters
        filtered_data = apply_filters(data, filter_settings)

        # Process leaderboard data
        if method == "Doc Sum":
            leaderboard_data = process_doc_sum(filtered_data, expression)
        elif method == "Classic":
            leaderboard_data = process_classic(filtered_data, expression)
        else:
            return JsonResponse({"error": "Unsupported method"}, status=400)

        return JsonResponse({"success": True, "leaderboard": leaderboard_data}, status=200)

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)
    


def apply_filters(data, filter_settings):
    filter_in = filter_settings.get("filterIn")
    filter_out = filter_settings.get("filterOut")

    # Apply filterIn (only if it's not "None")
    if filter_in and filter_in != "None":
        data = [doc for doc in data if filter_in in doc.values()]

    # Apply filterOut if it contains a valid condition
    if filter_out:
        try:
            condition_key, condition_value = filter_out.replace("'", "").split("==")
            condition_key = condition_key.strip()
            condition_value = condition_value.strip()

            data = [doc for doc in data if doc.get(condition_key) != condition_value]
        except Exception as e:
            print(f"Error parsing filterOut condition: {filter_out}, Error: {e}")

    return data


def process_doc_sum(data, expression):
    sum_field = expression.get("sumField", "") 
    display_field = expression.get("displayField", "")  

    leaderboard = {}

    for doc in data:
        key = doc.get(display_field) 
        value = doc.get(sum_field, 0)  

        if key:
            # Sum up values for the same key
            leaderboard[key] = leaderboard.get(key, 0) + (value if isinstance(value, (int, float)) else 0)

    return sorted(leaderboard.items(), key=lambda x: x[1], reverse=True)


def process_classic(data, expression):
    value_field = expression.get("valueField", "")
    display_field = expression.get("displayField", "")

    leaderboard = {}
    for doc in data:
        key = doc.get(display_field)
        value = doc.get(value_field, 0)
        if key:
            leaderboard[key] = value

    return sorted(leaderboard.items(), key=lambda x: x[1], reverse=True)


@csrf_exempt
def generate_30_days_leaderboard(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        
        if not board_id or not clerk_id:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        data_settings = data_collection.find_one({"board_id": board_id, "creator_id": clerk_id})
        if not data_settings:
            return JsonResponse({"error": "Data settings not found"}, status=404)
        
        source = data_settings.get("source")
        api_data = data_settings.get("api", {})
        filter_settings = data_settings.get("filter_settings", {})
        method = data_settings.get("method", "Doc Sum")
        expression = data_settings.get("expression", {})
        date_field = data_settings.get("date_settings", {}).get("dateField", "created_at")
        date_format = data_settings.get("date_settings", {}).get("dateFormat", "MM/DD/YY")
        
        # Convert format from user-friendly to Python strftime format
        format_mapping = {
            "MM/DD/YY": "%m/%d/%y",
            "MM/DD/YYYY": "%m/%d/%Y",
            "DD/MM/YY": "%d/%m/%y",
            "DD/MM/YYYY": "%d/%m/%Y",
            "YYYY-MM-DD": "%Y-%m-%d",
            "YY-MM-DD": "%y-%m-%d"
        }
        python_date_format = format_mapping.get(date_format, "%m/%d/%y")
        
        # Calculate the date 30 days ago (timezone naive)
        thirty_days_ago = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=30)).replace(tzinfo=None)
        print(f"Filtering data from: {thirty_days_ago}")
        
        def parse_date(date_str):
            """Helper function to parse dates with error handling"""
            try:
                return datetime.datetime.strptime(date_str, python_date_format)
            except (ValueError, TypeError) as e:
                print(f"Error parsing date '{date_str}': {e}")
                return None
        
        data = []
        if source == "MongoDB":
            client = MongoClient(api_data.get("uri"))
            db = client[api_data.get("database")]
            collection = db[api_data.get("collection")]
            
            all_docs = list(collection.find({}))
            data = [doc for doc in all_docs if 
                   parse_date(doc.get(date_field)) and 
                   parse_date(doc.get(date_field)) >= thirty_days_ago]
            
        elif source == "Supabase":
            supabase: Client = create_client(api_data.get("url"), api_data.get("anonKey"))
            # First get all records, then filter in Python
            response = supabase.table(api_data.get("table")).select("*").execute()
            all_docs = response.data
            data = [doc for doc in all_docs if 
                   parse_date(doc.get(date_field)) and 
                   parse_date(doc.get(date_field)) >= thirty_days_ago]
            
        elif source == "Firebase":
            if not firebase_admin._apps:
                cred = credentials.Certificate({
                    "apiKey": api_data.get("apiKey"),
                    "authDomain": api_data.get("authDomain"),
                    "projectId": api_data.get("projectId")
                })
                firebase_admin.initialize_app(cred)
            db = firestore.client()
            # Get all documents and filter in Python
            docs = db.collection(api_data.get("collection")).stream()
            all_docs = [doc.to_dict() for doc in docs]
            data = [doc for doc in all_docs if 
                   parse_date(doc.get(date_field)) and 
                   parse_date(doc.get(date_field)) >= thirty_days_ago]
            
        elif source == "Sheet":
            gc = gspread.service_account(filename="path_to_google_credentials.json")
            sheet = gc.open_by_url(api_data.get("url"))
            worksheet = sheet.sheet1
            all_records = worksheet.get_all_records()
            data = [doc for doc in all_records if 
                   parse_date(doc.get(date_field)) and 
                   parse_date(doc.get(date_field)) >= thirty_days_ago]
            
        else:
            return JsonResponse({"error": "Unsupported data source"}, status=400)
        
        print(f"Raw data length after date filtering: {len(data)}")
        if data:
            dates = [parse_date(d[date_field]) for d in data if parse_date(d[date_field])]
            print(f"Date range: from {min(dates)} to {max(dates)}")
        print(f"Sample first record: {data[0] if data else None}")
        
        # Apply filters
        filtered_data = apply_filters(data, filter_settings)
        print(f"Data length after filtering: {len(filtered_data)}")
        print(f"Sample filtered record: {filtered_data[0] if filtered_data else None}")
        
        # Process leaderboard data
        if method == "Doc Sum":
            leaderboard_data = process_doc_sum(filtered_data, expression)
        elif method == "Classic":
            leaderboard_data = process_classic(filtered_data, expression)
        else:
            return JsonResponse({"error": "Unsupported method"}, status=400)
        
        print(f"Final leaderboard data length: {len(leaderboard_data)}")
        print(f"Sample leaderboard entry: {leaderboard_data[0] if leaderboard_data else None}")
        
        return JsonResponse({"success": True, "leaderboard": leaderboard_data}, status=200)
        
    except json.JSONDecodeError:
        print("Error: Invalid JSON in request body")
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)
    


@csrf_exempt
def public_generate_30_days_leaderboard(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    
    try:
        data = json.loads(request.body)
        board = data.get("board")
        
        if not board:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        existing_board = boards_collection.find_one({"title": board})
        if not existing_board:
            return JsonResponse({"error": "Board document not found"}, status=404)
        
        data_settings = data_collection.find_one({"board_id": str(existing_board["_id"])})
        if not data_settings:
            return JsonResponse({"error": "Data settings not found"}, status=404)
        
        source = data_settings.get("source")
        api_data = data_settings.get("api", {})
        filter_settings = data_settings.get("filter_settings", {})
        method = data_settings.get("method", "Doc Sum")
        expression = data_settings.get("expression", {})
        date_field = data_settings.get("date_settings", {}).get("dateField", "created_at")
        date_format = data_settings.get("date_settings", {}).get("dateFormat", "MM/DD/YY")
        
        # Convert format from user-friendly to Python strftime format
        format_mapping = {
            "MM/DD/YY": "%m/%d/%y",
            "MM/DD/YYYY": "%m/%d/%Y",
            "DD/MM/YY": "%d/%m/%y",
            "DD/MM/YYYY": "%d/%m/%Y",
            "YYYY-MM-DD": "%Y-%m-%d",
            "YY-MM-DD": "%y-%m-%d"
        }
        python_date_format = format_mapping.get(date_format, "%m/%d/%y")
        
        # Calculate the date 30 days ago (timezone naive)
        thirty_days_ago = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=30)).replace(tzinfo=None)
        print(f"Filtering data from: {thirty_days_ago}")
        
        def parse_date(date_str):
            """Helper function to parse dates with error handling"""
            try:
                return datetime.datetime.strptime(date_str, python_date_format)
            except (ValueError, TypeError) as e:
                print(f"Error parsing date '{date_str}': {e}")
                return None
        
        data = []
        if source == "MongoDB":
            client = MongoClient(api_data.get("uri"))
            db = client[api_data.get("database")]
            collection = db[api_data.get("collection")]
            
            all_docs = list(collection.find({}))
            data = [doc for doc in all_docs if 
                   parse_date(doc.get(date_field)) and 
                   parse_date(doc.get(date_field)) >= thirty_days_ago]
            
        elif source == "Supabase":
            supabase: Client = create_client(api_data.get("url"), api_data.get("anonKey"))
            # First get all records, then filter in Python
            response = supabase.table(api_data.get("table")).select("*").execute()
            all_docs = response.data
            data = [doc for doc in all_docs if 
                   parse_date(doc.get(date_field)) and 
                   parse_date(doc.get(date_field)) >= thirty_days_ago]
            
        elif source == "Firebase":
            if not firebase_admin._apps:
                cred = credentials.Certificate({
                    "apiKey": api_data.get("apiKey"),
                    "authDomain": api_data.get("authDomain"),
                    "projectId": api_data.get("projectId")
                })
                firebase_admin.initialize_app(cred)
            db = firestore.client()
            # Get all documents and filter in Python
            docs = db.collection(api_data.get("collection")).stream()
            all_docs = [doc.to_dict() for doc in docs]
            data = [doc for doc in all_docs if 
                   parse_date(doc.get(date_field)) and 
                   parse_date(doc.get(date_field)) >= thirty_days_ago]
            
        elif source == "Sheet":
            gc = gspread.service_account(filename="path_to_google_credentials.json")
            sheet = gc.open_by_url(api_data.get("url"))
            worksheet = sheet.sheet1
            all_records = worksheet.get_all_records()
            data = [doc for doc in all_records if 
                   parse_date(doc.get(date_field)) and 
                   parse_date(doc.get(date_field)) >= thirty_days_ago]
            
        else:
            return JsonResponse({"error": "Unsupported data source"}, status=400)
        
        print(f"Raw data length after date filtering: {len(data)}")
        if data:
            dates = [parse_date(d[date_field]) for d in data if parse_date(d[date_field])]
            print(f"Date range: from {min(dates)} to {max(dates)}")
        print(f"Sample first record: {data[0] if data else None}")
        
        # Apply filters
        filtered_data = apply_filters(data, filter_settings)
        print(f"Data length after filtering: {len(filtered_data)}")
        print(f"Sample filtered record: {filtered_data[0] if filtered_data else None}")
        
        # Process leaderboard data
        if method == "Doc Sum":
            leaderboard_data = process_doc_sum(filtered_data, expression)
        elif method == "Classic":
            leaderboard_data = process_classic(filtered_data, expression)
        else:
            return JsonResponse({"error": "Unsupported method"}, status=400)
        
        print(f"Final leaderboard data length: {len(leaderboard_data)}")
        print(f"Sample leaderboard entry: {leaderboard_data[0] if leaderboard_data else None}")
        
        return JsonResponse({"success": True, "leaderboard": leaderboard_data}, status=200)
        
    except json.JSONDecodeError:
        print("Error: Invalid JSON in request body")
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)



# STRIPE CHECKOUT STUFF

# Set Stripe API key
stripe.api_key = settings.STRIPE_SK

# Stripe webhook secret
WEBHOOK_SECRET = settings.STRIPE_WEBHOOK_SECRET

@csrf_exempt
def create_checkout_session(request):
    """
    Creates a Stripe Checkout Session for recurring monthly payments.
    """
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            product_id = data.get("product_id")
            user_id = data.get("user_id")
            print(user_id)

            # Product price mapping for recurring subscriptions
            product_to_price_mapping = {
                "prod_RkfoQuyM98ny66": "price_1QrASzPAkbKeAZBD5OC058Jg",  # Recurring monthly price ID
            }

            if product_id not in product_to_price_mapping:
                return JsonResponse({"error": "Invalid Product ID"}, status=400)

            # Create a Stripe Checkout Session for recurring payments
            session = stripe.checkout.Session.create(
                payment_method_types=["card"],
                line_items=[
                    {
                        "price": product_to_price_mapping[product_id],
                        "quantity": 1,
                    }
                ],
                mode="subscription",  # Recurring subscription mode
                success_url="http://localhost:3000/dashboard",
                cancel_url="http://localhost:3000/dashboard",
                metadata={
                    "user_id": user_id,  # Attach user ID as metadata
                    "product_id": product_id,  # Attach product ID as metadata
                }
            )

            return JsonResponse({"url": session.url})

        except Exception as e:
            print(traceback.format_exc())
            return JsonResponse({"error": str(e)}, status=400)

@csrf_exempt
def stripe_webhook(request):
    """
    Handles Stripe webhook events.
    """
    payload = request.body
    sig_header = request.META['HTTP_STRIPE_SIGNATURE']

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError as e:
        # Signature doesn't match
        return JsonResponse({'error': 'Invalid signature'}, status=400)

    # Handle checkout.session.completed
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        handle_checkout_session(session)

    return JsonResponse({"status": "success"}, status=200)


def handle_checkout_session(session):
    """
    Processes the checkout session completion event.
    """
    user_id = session["metadata"].get("user_id")
    product_id = session["metadata"].get("product_id")

    if user_id:
        user = users_collection.find_one({'clerk_id': user_id})
    else:
        print("no user id")    

    if user and product_id=="prod_RkfoQuyM98ny66":
        try:
            users_collection.update_one({'clerk_id': user_id}, {
                        '$set': {'plan': 'pro'}
                    })
            print(f"Added Pro Plan to user {user_id}.")
        except Exception as e:
            print(f"Failed to update MongoDB: {e}")