from django.http import HttpResponse, JsonResponse, HttpRequest
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
import requests
import stripe
import gspread
from supabase import create_client, Client
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd


client = MongoClient(f'{settings.MONGO_URI}')
db = client['Competiboard']
boards_collection = db['Boards']
users_collection = db['Users']
data_collection = db['Data']
rewards_collection = db['Rewards']
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
def user_details(request):
    try:
        data = json.loads(request.body)
        clerk_id = data.get("clerk_id")
        
        if not clerk_id:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing user document
        user = users_collection.find_one({"clerk_id": clerk_id})
        if not user:
            return JsonResponse({"error": "User document not found"}, status=404)
        
        # Convert _id to string for JSON serialization
        user['_id'] = str(user['_id'])

        # Check if last_paid exists
        last_paid = user.get("last_paid")
        
        # Define the time threshold (1.5 months ago)
        time_threshold = datetime.datetime.today() - timedelta(days=45)

        if last_paid and last_paid >= time_threshold:
            user['plan'] = "pro"
        else:
            # Update plan to 'free' in MongoDB
            users_collection.update_one(
                {"clerk_id": clerk_id},
                {"$set": {"plan": "free"}}
            )
            user['plan'] = "free"  # Reflect this in the response
        
        return JsonResponse({"data": user}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)


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
        
        if user['num_boards'] >= 3 and user['plan'] != 'pro':
            return JsonResponse({'pro': 'Need pro for more than 3 boards'}, status=200);
        
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
            "method": {},
            "expression": {},
        }

        reward = {
            "creator_id": clerk_id, 
            "creator_name": user['name'],
            "email_field": "",
            "email_body": "",
        }

        created_data = data_collection.insert_one(data)
        data_id = created_data.inserted_id

        created_reward = rewards_collection.insert_one(reward)
        reward_id = created_reward.inserted_id

        board = {
            "creator_id": clerk_id,
            "creator_name": user['name'],
            'title': title,
            "created_at": date,
            "domain": "",
            "published": False,
            "thumbnail": s3_url, 
            "data": str(data_id),
            "reward": str(reward_id),
            "display": {'borders': "",
                        'boardBackground': "",
                        'pageBackground': "",
                        'titleColor': "",
                        'subtitleColor': "",
                        'dateRange': "",
                        'tableHeaders': "",
                        'ranks': "",
                        'rankingField': "",
                        'nameField': "",
                        'title': "",
                        'subtitle': "",
                        'rankingTitle': "",
                        'nameTitle': "",
                        'titleFont': "",
                        'subtitleFont': "",
                        'boardRankTitleFont': "",
                        'boardRankFont': "",
                        'boardNameTitleFont': "",
                        'boardNameFont': "",
                    }
        }

        created_board = boards_collection.insert_one(board)
        board_id = created_board.inserted_id

        data_collection.update_one(
            {'_id': data_id},
            {'$set': {'board_id': str(board_id)}}
        )

        rewards_collection.update_one(
            {'_id': reward_id},
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
        
        clerk_id = existing_board["creator_id"]
        
        # Find the existing data document
        user = users_collection.find_one({"clerk_id": clerk_id})
        if not user:
            return JsonResponse({"error": "User document not found"}, status=404)
        
        user['_id'] = str(user['_id'])
        
        existing_board['_id'] = str(existing_board['_id'])
        existing_board['creator_id'] = ""
        
        
        return JsonResponse({"data": existing_board, "user_details": user}, status=200)
    
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
            # Convert Google Sheets URL to CSV export URL
            sheet_id = api_data.get("url").split('/')[5]  # Extract sheet ID from URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
            
            # Read data using pandas
            df = pd.read_csv(csv_url)
            
            # Ensure we have column headers
            if df.empty or df.columns.empty:
                return JsonResponse({"error": "Sheet is empty or missing headers"}, status=400)
            
            # Clean column headers - remove whitespace and special characters
            df.columns = df.columns.str.strip()
            
            # Convert DataFrame to list of dicts
            data = df.to_dict('records')
            
            # Clean up the data and ensure consistent format
            cleaned_data = []
            for row in data:
                cleaned_row = {}
                for key, value in row.items():
                    # Only include non-empty values
                    if pd.notna(value) and str(value).strip() != '':
                        # Convert numeric types to match MongoDB format
                        if isinstance(value, (int, float)):
                            cleaned_row[key] = float(value) if isinstance(value, float) else int(value)
                        else:
                            cleaned_row[key] = str(value).strip()
                if cleaned_row:  # Only include rows with data
                    cleaned_data.append(cleaned_row)
            
            data = cleaned_data
            
            if not data:
                return JsonResponse({"error": "No valid data found in sheet"}, status=400)

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

        return JsonResponse({"success": True, "leaderboard": leaderboard_data[:50]}, status=200)

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
            # Convert Google Sheets URL to CSV export URL
            sheet_id = api_data.get("url").split('/')[5]  # Extract sheet ID from URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
            
            # Read data using pandas
            df = pd.read_csv(csv_url)
            
            # Ensure we have column headers
            if df.empty or df.columns.empty:
                return JsonResponse({"error": "Sheet is empty or missing headers"}, status=400)
            
            # Clean column headers - remove whitespace and special characters
            df.columns = df.columns.str.strip()
            
            # Convert DataFrame to list of dicts
            data = df.to_dict('records')
            
            # Clean up the data and ensure consistent format
            cleaned_data = []
            for row in data:
                cleaned_row = {}
                for key, value in row.items():
                    # Only include non-empty values
                    if pd.notna(value) and str(value).strip() != '':
                        # Convert numeric types to match MongoDB format
                        if isinstance(value, (int, float)):
                            cleaned_row[key] = float(value) if isinstance(value, float) else int(value)
                        else:
                            cleaned_row[key] = str(value).strip()
                if cleaned_row:  # Only include rows with data
                    cleaned_data.append(cleaned_row)
            
            data = cleaned_data
            
            if not data:
                return JsonResponse({"error": "No valid data found in sheet"}, status=400)

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

        return JsonResponse({"success": True, "leaderboard": leaderboard_data[:50]}, status=200)

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
    
    print(f"Data after filterIn: {data}")  # Debugging step

    # Apply filterOut if it contains a valid condition
    if filter_out:
        try:
            condition_key, condition_value = filter_out.replace("'", "").split("==")
            condition_key = condition_key.strip()
            condition_value = condition_value.strip()
            
            # Apply filterOut condition safely
            data = [doc for doc in data if str(doc.get(condition_key)) != condition_value]

            print(f"Data after filterOut: {data}")
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
            # Convert Google Sheets URL to CSV export URL
            sheet_id = api_data.get("url").split('/')[5]  # Extract sheet ID from URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
            
            # Read data using pandas
            df = pd.read_csv(csv_url)
            
            # Ensure we have column headers
            if df.empty or df.columns.empty:
                return JsonResponse({"error": "Sheet is empty or missing headers"}, status=400)
            
            # Clean column headers - remove whitespace and special characters
            df.columns = df.columns.str.strip()
            
            # Convert DataFrame to list of dicts
            data = df.to_dict('records')
            
            # Clean up the data and ensure consistent format
            cleaned_data = []
            for row in data:
                cleaned_row = {}
                for key, value in row.items():
                    # Only include non-empty values
                    if pd.notna(value) and str(value).strip() != '':
                        # Convert numeric types to match MongoDB format
                        if isinstance(value, (int, float)):
                            cleaned_row[key] = float(value) if isinstance(value, float) else int(value)
                        else:
                            cleaned_row[key] = str(value).strip()
                if cleaned_row:  # Only include rows with data
                    cleaned_data.append(cleaned_row)
            
            if not cleaned_data:
                return JsonResponse({"error": "No valid data found in sheet"}, status=400)
            
            data = [doc for doc in cleaned_data if 
                     date_field in doc and 
                     parse_date(doc[date_field]) and 
                     parse_date(doc[date_field]) >= thirty_days_ago]

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
        
        return JsonResponse({"success": True, "leaderboard": leaderboard_data[:50]}, status=200)
        
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
            # Convert Google Sheets URL to CSV export URL
            sheet_id = api_data.get("url").split('/')[5]  # Extract sheet ID from URL
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
            
            # Read data using pandas
            df = pd.read_csv(csv_url)
            
            # Ensure we have column headers
            if df.empty or df.columns.empty:
                return JsonResponse({"error": "Sheet is empty or missing headers"}, status=400)
            
            # Clean column headers - remove whitespace and special characters
            df.columns = df.columns.str.strip()
            
            # Convert DataFrame to list of dicts
            data = df.to_dict('records')
            
            # Clean up the data and ensure consistent format
            cleaned_data = []
            for row in data:
                cleaned_row = {}
                for key, value in row.items():
                    # Only include non-empty values
                    if pd.notna(value) and str(value).strip() != '':
                        # Convert numeric types to match MongoDB format
                        if isinstance(value, (int, float)):
                            cleaned_row[key] = float(value) if isinstance(value, float) else int(value)
                        else:
                            cleaned_row[key] = str(value).strip()
                if cleaned_row:  # Only include rows with data
                    cleaned_data.append(cleaned_row)
            
            if not cleaned_data:
                return JsonResponse({"error": "No valid data found in sheet"}, status=400)
            
            data = [doc for doc in cleaned_data if 
                     date_field in doc and 
                     parse_date(doc[date_field]) and 
                     parse_date(doc[date_field]) >= thirty_days_ago]
            
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
        
        return JsonResponse({"success": True, "leaderboard": leaderboard_data[:50]}, status=200)
        
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
                "prod_RmO52yWy4eNtNq": "price_1QspJAAszcsVQ3TAx8czqoUs",  # Recurring monthly price ID
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
                success_url="https://competiboard.com/dashboard",
                cancel_url="https://competiboard.com/dashboard",
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

    if user and product_id=="prod_RmO52yWy4eNtNq":
        try:
            users_collection.update_one({'clerk_id': user_id}, {
                        '$set': {'plan': 'pro', 'last_paid': datetime.datetime.today()}
                    })
            print(f"Added Pro Plan to user {user_id}.")
        except Exception as e:
            print(f"Failed to update MongoDB: {e}")


@csrf_exempt
def update_reward(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        email_field = data.get("email_field")
        email_body = data.get("email_body")
        
        if not board_id or not clerk_id or not email_field or not email_body:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing data document
        existing_reward = rewards_collection.find_one({"board_id": board_id, "creator_id": clerk_id})
        if not existing_reward:
            return JsonResponse({"error": "Reward document not found"}, status=400)
        
        # Update the document
        rewards_collection.update_one(
            {"board_id": board_id, "creator_id": clerk_id},
            {"$set": {"email_field": email_field, "email_body": email_body}}
        )
        
        return JsonResponse({"success": True, "message": "Reward updated successfully"}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)
    

@csrf_exempt
def reward_details(request):
    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        
        if not board_id or not clerk_id:
            return JsonResponse({"error": "Missing required fields"}, status=400)
        
        # Find the existing data document
        existing_reward = rewards_collection.find_one({"board_id": board_id, "creator_id": clerk_id})
        if not existing_reward:
            return JsonResponse({"error": "Data document not found"}, status=404)
        
        existing_reward['_id'] = str(existing_reward['_id'])
        
        return JsonResponse({"data": existing_reward}, status=200)
    
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)
    

@csrf_exempt
def send_rewards(request):
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)

    try:
        data = json.loads(request.body)
        board_id = data.get("board_id")
        clerk_id = data.get("clerk_id")
        time_range = data.get("time")
        min_rank = int(data.get("min_rank"))
        max_rank = int(data.get("max_rank"))

        if not all([board_id, clerk_id, time_range, min_rank, max_rank]):
            return JsonResponse({"error": "Missing required fields"}, status=400)

        # Get reward settings
        reward_settings = rewards_collection.find_one({
            "board_id": board_id,
            "creator_id": clerk_id
        })
        if not reward_settings:
            return JsonResponse({"error": "Reward settings not found"}, status=404)

        # Get data source configuration
        data_settings = data_collection.find_one({
            "board_id": board_id,
            "creator_id": clerk_id
        })
        if not data_settings:
            return JsonResponse({"error": "Data settings not found"}, status=404)

        # Generate leaderboard
        lb_request = HttpRequest()
        lb_request.method = "POST"
        lb_request.content_type = "application/json"
        lb_request._body = json.dumps({
            "board_id": board_id,
            "clerk_id": clerk_id
        })

        lb_response = generate_30_days_leaderboard(lb_request) if time_range == "30" else generate_leaderboard(lb_request)
        if lb_response.status_code != 200:
            return lb_response

        leaderboard = json.loads(lb_response.content)["leaderboard"]
        group_by_field = data_settings.get("expression", {}).get("displayField", "username")
        source_type = data_settings.get("source")
        api_config = data_settings.get("api", {})

        # Connect to data source
        data_source = connect_to_source(source_type, api_config)
        ranked_users = []
        current_rank = 1
        previous_score = None
        actual_position = 0

        sorted_users = sorted(leaderboard, key=lambda x: x[1], reverse=True)
        for user in sorted_users:
            actual_position += 1
            if user[1] != previous_score:
                current_rank = actual_position
                previous_score = user[1]

            if current_rank > max_rank:
                break

            if current_rank >= min_rank:
                username = user[0]
                if not username:
                    continue

                # Get user email from original data source
                email = get_user_email(
                    source_type,
                    data_source,
                    group_by_field,
                    str(username),
                    reward_settings["email_field"],
                    api_config
                )

                if email:
                    ranked_users.append({
                        "rank": current_rank,
                        "email": email,
                        "data": user
                    })

        # Mailgun email sending
        mailgun_domain = settings.MAILGUN_DOMAIN
        mailgun_api_key = settings.MAILGUN_API_KEY
        sent_emails = []
        failed_emails = []

        for user in ranked_users:
            try:
                email_body = reward_settings["email_body"]

                response = requests.post(
                    f"https://api.mailgun.net/v3/{mailgun_domain}/messages",
                    auth=("api", mailgun_api_key),
                    data={
                        "from": f"Competiboard <mailgun@{mailgun_domain}>",
                        "to": [user["email"]],
                        "subject": "Congrats for placing on the leaderboard!",
                        "text": email_body
                    }
                )

                if response.status_code == 200:
                    sent_emails.append(user["email"])
                else:
                    failed_emails.append({
                        "email": user["email"],
                        "error": response.json().get("message", "Unknown error")
                    })

            except Exception as e:
                failed_emails.append({
                    "email": user["email"],
                    "error": str(e)
                })

        return JsonResponse({
            "success": True,
            "sent_count": len(sent_emails),
            "failed_count": len(failed_emails),
            "sent_emails": sent_emails,
            "failed_emails": failed_emails
        })

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        print(traceback.format_exc())
        return JsonResponse({"error": str(e)}, status=500)


def connect_to_source(source_type, api_config):
    """Connect to the original data source"""
    if source_type == "MongoDB":
        client = MongoClient(api_config.get("uri"))
        return client[api_config.get("database")][api_config.get("collection")]
    elif source_type == "Supabase":
        return create_client(api_config.get("url"), api_config.get("anonKey"))
    elif source_type == "Firebase":
        if not firebase_admin._apps:
            cred = credentials.Certificate({
                "apiKey": api_config.get("apiKey"),
                "authDomain": api_config.get("authDomain"),
                "projectId": api_config.get("projectId")
            })
            firebase_admin.initialize_app(cred)
        return firestore.client()
    elif source_type == "Sheet":
        sheet_id = api_config.get("url").split('/')[5]  # Extract sheet ID from URL
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        return csv_url
    
    return None

def get_user_email(source_type, data_source, group_field, username, email_field, api_config):
    """Query the data source to find user's email"""
    try:
        if source_type == "MongoDB":
            user = data_source.find_one({group_field: username})
            return user.get(email_field) if user else None
        elif source_type == "Supabase":
            response = data_source.table(api_config.get("table")) \
                .select(email_field) \
                .eq(group_field, username) \
                .execute()
            return response.data[0].get(email_field) if response.data else None
        elif source_type == "Firebase":
            docs = data_source.collection(api_config.get("collection")) \
                .where(group_field, "==", username) \
                .limit(1) \
                .stream()
            return next(docs).to_dict().get(email_field) if docs else None
        elif source_type == "Sheet":
            # Read data using pandas
            df = pd.read_csv(data_source)
            
            # Ensure we have column headers
            if df.empty or df.columns.empty:
                return JsonResponse({"error": "Sheet is empty or missing headers"}, status=400)
            
            # Clean column headers - remove whitespace and special characters
            df.columns = df.columns.str.strip()
            
            # Convert DataFrame to list of dicts
            data = df.to_dict('records')
            
            # Clean up the data and ensure consistent format
            cleaned_data = []
            for row in data:
                cleaned_row = {}
                for key, value in row.items():
                    # Only include non-empty values
                    if pd.notna(value) and str(value).strip() != '':
                        # Convert numeric types to match MongoDB format
                        if isinstance(value, (int, float)):
                            cleaned_row[key] = float(value) if isinstance(value, float) else int(value)
                        else:
                            cleaned_row[key] = str(value).strip()
                if cleaned_row:  # Only include rows with data
                    cleaned_data.append(cleaned_row)
            
            if not cleaned_data:
                return JsonResponse({"error": "No valid data found in sheet"}, status=400)
            
            data = cleaned_data
            for record in data:
                if str(record.get(group_field, "")) == username:
                    print(record.get(email_field))
                    return record.get(email_field)
                
        return None
    except Exception as e:
        print(f"Error fetching email for {username}: {str(e)}")
        return None
    