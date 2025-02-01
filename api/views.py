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