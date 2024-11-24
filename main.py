from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Annotated
import models
from database import engine, SessionLocal
from sqlalchemy.orm import Session
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd
from haversine import haversine

cred = credentials.Certificate('restau-5dba7-firebase-adminsdk-jpame-be78ad3e26.json')
app = firebase_admin.initialize_app(cred)
firestoreDB = firestore.client()

app = FastAPI()
models.Base.metadata.create_all(bind=engine)

"""
class ChoiceBase(BaseModel):
    choice_text: str
    is_correct: bool


class QuestionBase(BaseModel):
    question_text: str
    choices: List[ChoiceBase]
""" 

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

db_dependency = Annotated[Session, Depends(get_db)]

"""
@app.get("/questions/{question_id}")
async def read_question(question_id: int, db: db_dependency):
    result = db.query(models.Questions).filter(models.Questions.id == question_id).first()
    if not result:
        raise HTTPException(status_code=404, detail="Question not found")
    else:
        return result


@app.post("/questions/")
async def create_questions(question: QuestionBase, db: db_dependency):
    db_question = models.Questions(question_text = question.question_text)
    db.add(db_question)
    db.commit()
    db.refresh(db_question)
    for choice in question.choices:
        db_choice = models.Choices(choice_text = choice.choice_text, is_correct = choice.is_correct, question_id = db_question.id)
        db.add(db_choice)
        db.commit()
"""

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/screentimes/clean")
async def root(db: db_dependency):
    db.query(models.LastVisit).delete()
    db.query(models.WeekScreenTime).delete()
    db_visit = models.LastVisit(id = 1, year = 0, week = 0)
    db.merge(db_visit)
    db.commit()
    return {
        "response": "Database Successfully cleaned"
    }

@app.get("/screentimes")
async def setup(db: db_dependency):

    #Retriving cached moment of visit
    visitRetrieved = db.query(models.LastVisit).filter(models.LastVisit.id == 1).first()

    latestWeek = visitRetrieved.week
    latestYear = visitRetrieved.year

    #Getting current moment
    currentWeek = pd.Timestamp.now().week
    currentYear = pd.Timestamp.now().year

    #Checking if the cached answer is to be used
    if (latestYear != currentYear or latestWeek != currentWeek):

        print("Retrieving answer...")

        #Firebase events retrieval
        docs = firestoreDB.collection("screen_time_events").get()

        #Turning documents into a pandas dataframe
        dictArray = []
        for doc in docs:
            dictArray.append(doc.to_dict())
        df = pd.DataFrame(dictArray)

        #Ordering and getting the necessary data
        df["week"] = df["date"].transform(lambda date: date.week)
        df["year"] = df["date"].transform(lambda date: date.year)
        df["week"] = df["year"].astype(str) + "-" + df["week"].astype(str)
        cols = ["screen", "user_id", "week", "time_seconds"]
        df = df[cols]

        #Performing the analysis process
        grouped = df.groupby(["screen", "user_id", "week"])
        firstgrouped = grouped.sum()
        grouped = firstgrouped.groupby(["screen", "user_id"])
        secondgrouped = grouped.mean()
        grouped = secondgrouped.groupby("screen")
        answer = grouped.mean()
        #Saving the answer in local storage
        for index, row in answer.iterrows():
            db_screen = models.WeekScreenTime(screen = index, time = float(row["time_seconds"]))
            db.merge(db_screen)
            db.commit()
    
    #Lastest visit update
    db_visit = models.LastVisit(id = 1, year = currentYear, week = currentWeek)
    db.merge(db_visit)
    db.commit()

    #Answer building
    answer = {}
    dataRetrieval = db.query(models.WeekScreenTime).order_by(models.WeekScreenTime.time).all()
    for entry in dataRetrieval:
        answer[entry.screen] = entry.time
    return answer


@app.get("/navigation-paths")
async def setup():

    # Add a path to the tree
    def add_path(tree, path):
        current_node = tree
        for i in range(len(path)):
            screen = path[i]
           
            if screen not in current_node:
                current_node[screen] = {"count": 0, "next": {}, "end_count": 0}

            current_node[screen]["count"] += 1

            if i == len(path) - 1:
                current_node[screen]["end_count"] += 1
            else:
                current_node = current_node[screen]["next"]

    # Collect traversed paths with at least 3 screens
    def collect_paths(tree, current_path, min_length=3):
        paths_with_counts = []
        
        # Internal function to traverse the tree and collect paths
        def traverse(current_node, current_path):
            for screen, data in current_node.items():
                new_path = current_path + [screen]
                if len(new_path) >= min_length:
                    paths_with_counts.append((new_path, data["count"]))
                traverse(data["next"], new_path)
        
        traverse(tree, current_path)
        return paths_with_counts

    # Get the top N most common paths
    def get_top_paths(tree, top_n=5, min_length=3):
        paths = collect_paths(tree, [], min_length=min_length)
        sorted_paths = sorted(paths, key=lambda x: x[1], reverse=True)
        return sorted_paths[:top_n]

    # Create the tree
    tree = {}

    # Fetch the navigation paths from the database
    data = firestoreDB.collection("navigation_paths").get()

    # Parse and add each path to the tree
    for d in data:
        d = d.to_dict()["path"].split(" > ")  # Example: "Home > Liked > Search"
        add_path(tree, d)

    # Get the top N paths and preserve order
    top_paths = get_top_paths(tree)

    # Convert paths to source-target transitions while preserving sequence
    transitions = []
    for path, count in top_paths:
        # Build source-target pairs with positional labels
        for i in range(len(path) - 1):
            source = f"{i+1}: {path[i]}"     # Add position to the source
            target = f"{i+2}: {path[i+1]}"   # Add position to the target
            transitions.append({
                "source": source,
                "target": target,
                "users": count
            })

    return transitions

@app.get("/nearbyxcuisine")
async def setup(userID, lat, lon):
    try:
        cuisines = set(firestoreDB.collection("Preference Tags").document("tags").get().to_dict()["Cuisine"]["list"])
        preferences = set(firestoreDB.collection("users").document(userID).get().to_dict()["preferences"])

        prefCuisine = cuisines.intersection(preferences)
        
        userLoc = (float(lat), float(lon))
        
        for restaurant in firestoreDB.collection("restaurants").get():
            restaurant = restaurant.to_dict()
            
            restCat = set(restaurant["categories"])     
            
            intersec = prefCuisine.intersection(restCat)
            
            if len(intersec)>0:
                restLoc = (float(restaurant['latitude']), float(restaurant['longitude']))
                distance = haversine(userLoc, restLoc)
                
                if distance < 1:
                    return list(intersec)[0]
    
    except:
        return None

@app.get("/reviewed_restaurant_percent")
async def reviewed_restaurant_percent(userID: str):
    try:
        user_reviews = firestoreDB.collection("reviews").where("authorId", "==", userID).get()
        
        reviewed_restaurants = {review.to_dict()["restaurantId"] for review in user_reviews}
        
        total_restaurants = firestoreDB.collection("restaurants").get()
        total_restaurant_count = len(total_restaurants)

        if total_restaurant_count > 0:
            review_percentage = int((len(reviewed_restaurants) / total_restaurant_count) * 100)
            return review_percentage
        else:
            return 0  

    except Exception as e:
        return {"error": str(e)}

    
@app.get("/restaurant_search_types/clean")
async def root(db: db_dependency):
    db.query(models.RestaurantTypes).delete()
    db.merge()
    db.commit()
    return {
        "response": "Database Successfully cleaned"
    }

@app.get("/restaurant_search_types")
async def get_restaurant_search_types(db: db_dependency):
    try:
        # Fetch documents from Firestore
        docs = firestoreDB.collection("restaurant_search_types").get()

        type_count = {}

        # Count the occurrences of each restaurant type
        for doc in docs:
            doc_data = doc.to_dict()
            for type_key in doc_data.keys():
                if type_key in type_count:
                    type_count[type_key] += 1
                else:
                    type_count[type_key] = 1

        if not type_count:
            return {"message": "No types found"}

        # Loop through the counted types and merge them into the SQL database
        for key, value in type_count.items():
            # Check if the type already exists
            existing_entry = db.query(models.RestaurantTypes).filter_by(resType=key).first()

            if existing_entry:
                existing_entry.count = value  # Update the count if it exists
            else:
                db_screen = models.RestaurantTypes(resType=key, count=value)
                db.add(db_screen)  # Insert new entry

        db.commit()  # Commit all changes after the loop
        return type_count

    except Exception as e:
        db.rollback()  # Rollback in case of error
        return {"error": str(e)}

@app.get("/FeaturesInteractions")
async def setup(db: db_dependency):

    docs = firestoreDB.collection("features_interactions").get()

    dictArray = []
    for doc in docs:
        dictArray.append(doc.to_dict())
    df = pd.DataFrame(dictArray)

    # Agrupar por 'nameFeatureInteraction' y contar el número de ocurrencias
    result = df.groupby('nameFeatureInteraction').size().reset_index(name='count')

    total_interactions = result['count'].sum()

    # for index, row in result.iterrows():
    #     db_featureinteraction = models.FeatureInteraction(
    #         featureName=row["nameFeatureInteraction"], 
    #         count=int(row["count"]), 
    #         datatime_data=datetime.utcnow(),
    #         percentage_uses = round((int(row["count"]) / total_interactions) * 100,2)
    #     )
    #     print(datetime.utcnow())
    #     db.merge(db_featureinteraction)
    # db.commit()

    answer = {}
    
    for index, row in result.iterrows():
        answer[row["nameFeatureInteraction"]] = int(row["count"])
        answer[row["nameFeatureInteraction"] + "Percentage"] = round((int(row["count"]) / total_interactions) * 100,2)

    most_used_feature = result.loc[result['count'].idxmax()]
    less_used_feature = result.loc[result['count'].idxmin()]

    answer["MostUsedFeature"] = most_used_feature["nameFeatureInteraction"]
    answer["LessUsedFeature"] = less_used_feature["nameFeatureInteraction"]

    return answer


@app.get("/popular_categories")
def popular_categories():

    users = firestoreDB.collection("users").get()
    print(len(users))

    categories = {}

    for user in users:
        user_dictionary = user.to_dict()
        if ("preferences" in user_dictionary):

            for category in user_dictionary["preferences"]:
                if (category in categories):
                    categories[category] = categories[category] + 1
                else:
                    categories[category] = 0
    
    categoryList = []
    for category in categories.keys():
        categoryItem = {}
        categoryItem["category"] = category
        categoryItem["value"] = categories[category]
        categoryList.append(categoryItem)

    categoryList = sorted(categoryList, key = lambda x: x["value"], reverse=True)
    return categoryList


@app.get("/like_review_week")
def most_liked_positive_reviewed_week(db: db_dependency):

    currentWeek = pd.Timestamp.now().week
    currentYear = pd.Timestamp.now().year

    current_week = str(currentYear) + "-" + str(currentWeek)

    likes = firestoreDB.collection("like_date_restaurant_event").get()
    restaurants = firestoreDB.collection("restaurants").get()
    
    reviews = firestoreDB.collection("reviews").get()

    restaurantsDicts = []
    for restaurant in restaurants:
        restaurant_dictionary = restaurant.to_dict()
        restaurant_dictionary["restaurantId"] = restaurant.id
        restaurantsDicts.append(restaurant_dictionary)
    df_rest = pd.DataFrame(restaurantsDicts)[["restaurantId", "name"]]


    reviewsDicts = []
    for review in reviews:
        review_dictionary = review.to_dict()
        review_dictionary["id"] = review.id
        reviewsDicts.append(review_dictionary)
    df_reviews = pd.DataFrame(reviewsDicts)[["date", "restaurantId", "rating"]]

    likesDicts = []
    for like in likes:
        like_dictionary = like.to_dict()
        like_dictionary["id"] = like.id
        likesDicts.append(like_dictionary)
    df_likes = pd.DataFrame(likesDicts)

    df_likes["week"] = df_likes["date"].transform(lambda date: date.week)
    df_likes["year"] = df_likes["date"].transform(lambda date: date.year)
    df_likes["week"] = df_likes["year"].astype(str) + "-" + df_likes["week"].astype(str)
    df_likes = df_likes[["restaurantId", "week"]]

    df_reviews["week"] = df_reviews["date"].transform(lambda date: date.week)
    df_reviews["year"] = df_reviews["date"].transform(lambda date: date.year)
    df_reviews["week"] = df_reviews["year"].astype(str) + "-" + df_reviews["week"].astype(str)
    df_reviews = df_reviews[["restaurantId", "rating", "week"]]

    df_reviews = (df_reviews[df_reviews['week'] == current_week])[["restaurantId", "rating"]]
    df_reviews = df_reviews[df_reviews['rating'] >= 2.5][["restaurantId"]]
    df_likes = df_likes[df_likes['week'] == current_week][["restaurantId"]]

    df_reviews["count_reviews"] = 1
    df_likes["count_likes"] = 1

    df_reviews = df_reviews.groupby(["restaurantId"])
    df_reviews = df_reviews.sum()

    df_likes = df_likes.groupby(["restaurantId"])
    df_likes = df_likes.sum()

    df_reviews = df_reviews.reset_index()
    df_likes = df_likes.reset_index()

    df_rest_reviews = pd.merge(df_rest, df_reviews, on='restaurantId', how='left')
    df_rest_reviews["count_reviews"] = df_rest_reviews["count_reviews"].fillna(0)

    df_rest_reviews_likes = pd.merge(df_rest_reviews, df_likes, on='restaurantId', how='left')
    df_rest_reviews_likes["count_likes"] = df_rest_reviews_likes["count_likes"].fillna(0)

    df_rest_reviews_likes = df_rest_reviews_likes.sort_values(by=["count_likes", "count_reviews"], ascending= [False, False])

    df_rest_reviews_likes = df_rest_reviews_likes[["name", "count_likes", "count_reviews"]]

    answer = []
    for _, row in df_rest_reviews_likes.iterrows():
        item = {}
        item["name"] = row["name"]
        item["count_likes"] = row["count_likes"]
        item["count_reviews"] = row["count_reviews"]
        answer.append(item)
    
    print(answer)
    return answer

@app.get("/restaurantCommonQualities")
def commonQualities(topN = 10):
    topN = int(topN)
    users = firestoreDB.collection("users").get()

    likes_count = {}
    for user in users:
        user_data = user.to_dict()
        if "likes" in user_data:
            for like in user_data["likes"]:
                if like in likes_count:
                    likes_count[like] += 1
                else:
                    likes_count[like] = 1
                    
    docs = (firestoreDB.collection("restaurants").stream())
    
    docList = []
    for doc in docs:
        docData = doc._data
        docData['id'] = doc.id
        docData['likeCount'] = likes_count[doc.id] if doc.id in likes_count else 0
        docList.append(docData)
        
    docList = sorted(docList, key=lambda x: x['likeCount'], reverse=True)
    
    categories = {}
    for doc in docList[:topN]:
        for cat in doc['categories']:
            if cat in categories:
                categories[cat] += 1
            else:
                categories[cat] = 1
    
    categories = {k: v for k, v in categories.items() if v > 2}
                
                
    return categories

@app.get("/AreaWithMoreLikedRestaurants")
async def setup(db: db_dependency): # type: ignore

    restaurants = firestoreDB.collection("restaurants").get()

    users = firestoreDB.collection("users").get()

    dictArray = []
    for restaurant in restaurants:
        rest_dict = {}
        rest_dict["placeName"] = restaurant.to_dict().get("placeName")
        rest_dict["doc_id"] = restaurant.id
        dictArray.append(rest_dict)
    df_restaurant = pd.DataFrame(dictArray)

    #print(df_restaurant)

    likesArray = []
    for user in users:
        user_dict = user.to_dict()
        if user_dict.get("likes") and len(user_dict.get("likes"))>0:
            likesArray.extend(user_dict.get("likes"))
    
    df_likes = pd.DataFrame(likesArray)   
    df_likes = df_likes.rename(columns={0: "doc_id"})

    df_likes.set_index("doc_id", inplace=True)
    df_restaurant.set_index("doc_id", inplace=True)

    result = df_likes.join(df_restaurant)

    result = result.groupby('placeName').size().reset_index(name='LikesCount')

    print(result)

    answer = {}
    
    for index, row in result.iterrows():
        answer[row["placeName"]] = int(row["LikesCount"])

    most_area_liked = result.loc[result['LikesCount'].idxmax()]
    least_area_liked = result.loc[result['LikesCount'].idxmin()]

    answer["MostAreaLiked"] = most_area_liked["placeName"]
    answer["LeastAreaLiked"] = least_area_liked["placeName"]
    return answer

@app.get("/weekly_maps_redirection")
async def setup():
    try:
        seven_days_ago = datetime.now() - timedelta(days=7)

        docs = (
            firestoreDB.collection("map_search_times")
            .where("time", ">=", seven_days_ago)
            .get()
        )

        count = len(docs)
        
        return {"count": count}
    except Exception as e:
        return {"error": str(e)}


@app.get("/random-review")
async def setup():

    data = firestoreDB.collection("random_review").get()

    leftReview = 0
    for d in data:
        d = d.to_dict()
        if d["left_review"]:
            leftReview += 1
    
    return leftReview/len(data)