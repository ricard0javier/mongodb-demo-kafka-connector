from pymongo import MongoClient

from config import Config


def main():
    """
    Main function for the Python project template.
    This function can be used as an entry point for your application.
    """
    print("===== Starting MongoDB Service Template =====")
    client = MongoClient(Config.MONGODB_URI)
    db = client["quickstart"]
    testCollection = db["sampleData"]
    testCollection.insert_one({"name": "John", "age": 30})
    cursor = testCollection.find({"name": "John"})
    print(list(cursor))


if __name__ == "__main__":
    main()
