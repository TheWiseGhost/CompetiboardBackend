from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from pymongo import MongoClient
from django.conf import settings
import boto3
from bson import ObjectId
import traceback
import datetime
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
        title = req.POST.get('title')
        my_file = req.FILES['file']

        date = datetime.datetime.today()

        key = f'thumbnails/{clerk_id}_{my_file.name}'

        s3.upload_fileobj(
            my_file,   # Local file path
            bucket_name,    
            key,
            ExtraArgs={'ACL': 'public-read'}
        )

        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{key}"
        

        if not clerk_id:
            print('No ClerkID')
            return JsonResponse({'error': 'clerk_id is required'})
        
        user = users_collection.find_one({'clerk_id': clerk_id})
        if not user:
            print('No User')
            return JsonResponse({'error': 'User not found'})


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

        # Get the ObjectId of the inserted board document
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
        update_result = data_collection.update_one(
            {"board_id": board_id, "creator_id": clerk_id},
            {"$set": {"method": method, "filter_settings": filter_settings, "date_settings": date_settings, "expression": expression}}
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
        update_result = boards_collection.update_one(
            {"_id": ObjectId(board_id), "creator_id": clerk_id},
            {"$set": {"display": settings}}
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
        
        filter_settings = data_settings.get("filter_settings", {})
        method = data_settings.get("method", "Doc Sum")
        expression = data_settings.get("expression", {})
        source = data_settings.get("source")
        api_data = data_settings.get("api", {})
        
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
                initialize_app(cred)
            
            db = firestore.client()
            docs = db.collection(api_data.get("collection")).stream()
            data = [doc.to_dict() for doc in docs]
        
        elif source == "Sheet":
            gc = gspread.Client()
            sheet = gc.open_by_url(api_data.get("url"))
            worksheet = sheet.sheet1
            records = worksheet.get_all_records()

            # Convert to list of dicts
            data = []
            for idx, row in enumerate(records):
                row_dict = {k: v for k, v in row.items() if v != ''}
                row_dict["row_number"] = idx + 2  # Add row number for reference
                data.append(row_dict)
        
        else:
            return JsonResponse({"error": "Unsupported data source"}, status=400)
        
        filtered_data = apply_filters(data, filter_settings)

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
    filtered_data = data
    if filter_settings.get("filterIn"):
        filtered_data = [doc for doc in filtered_data if filter_settings["filterIn"] in doc]
    if filter_settings.get("filterOut"):
        filtered_data = [doc for doc in filtered_data if filter_settings["filterOut"] not in doc]
    return filtered_data

def process_doc_sum(data, expression):
    sum_field = expression.get("sumField")
    display_field = expression.get("displayField")
    
    leaderboard = {}
    for doc in data:
        key = doc.get(display_field)
        value = doc.get(sum_field, 0)
        if key in leaderboard:
            leaderboard[key] += value
        else:
            leaderboard[key] = value
    
    return sorted(leaderboard.items(), key=lambda x: x[1], reverse=True)

def process_classic(data, expression):
    value_field = expression.get("valueField")
    display_field = expression.get("displayField")
    
    leaderboard = {}
    for doc in data:
        key = doc.get(display_field)
        value = doc.get(value_field, 0)
        leaderboard[key] = value
    
    return sorted(leaderboard.items(), key=lambda x: x[1], reverse=True)
