from faker import Faker
import mysql.connector as mysql
import random
from datetime import datetime, timedelta

def connect_db():
    # open a connection to the db
    db = mysql.connect(
        host = "localhost",
        port = "3306",
        user = "root",
        password = "6902",
        database = "ecommerce"
    )
    return db

def disconnect_db(db):
    # close the connection
    db.close()

def delete_all(db):
    # delete all records from db

    # open a cursor
    cursor = db.cursor()

    # delete all previous records from needed tables. Data from other tables will be deleted using "on delete cascade"
    cursor.execute(f"delete from orders")
    cursor.execute(f"delete from vendor_products")
    cursor.execute(f"delete from vendors")
    cursor.execute(f"delete from customers")
    cursor.execute(f"delete from products")
    cursor.execute(f"delete from categories")
    cursor.execute(f"delete from statuses")
    cursor.execute(f"delete from cities")

    # commit the changes
    db.commit()

def get_categories():
    # return all pre-defined categories
    categories = ["Sports", "Clothes", "Books", "Electronics", "Home", "Beauty", "Accessories", "Mobile", "Food", "Digital"]
    return categories

def get_statuses():
    # retruen all pre-defined statuses
    statuses = ["Incomplete", "Completed", "Shipped", "Delivered", "Not Delivered", "Returned", "Fraud"]
    return statuses

def insert_categories(db):
    # insert all categories into the db

    # open a cursor
    cursor = db.cursor()

    categories = get_categories()

    category_id = 1
    for category in categories:
        cursor.execute(f"insert into categories (category_id, name) values ({category_id}, '{category}')")
        category_id += 1 

    # commit the changes
    db.commit()
    
def insert_statuses(db):
    # insert all statuses into the db

    # open a cursor
    cursor = db.cursor()

    statuses = get_statuses()

    status_id = 1
    for status in statuses:
        cursor.execute(f"insert into statuses (status_id, name) values ({status_id}, '{status}')")
        status_id += 1 

    # commit the changes
    db.commit()


def insert_cities(db, city_count):
    # insert required number of cities into the db

    # open a cursor
    cursor = db.cursor()

    # instantiate a faker
    fake = Faker()

    city_id = 1
    for _ in range(city_count):
        cursor.execute(f"insert into cities (city_id, name) values ({city_id}, '{fake.unique.city()}')")
        city_id += 1 

    # commit the changes
    db.commit()

    
def insert_customers(db, customer_count, city_count):
    # insert required number of customers into the db

    # open a cursor
    cursor = db.cursor()

    # instantiate a faker
    fake = Faker()

    customer_id = 1
    for _ in range(customer_count):
        is_member = random.randint(0,1)
        cursor.execute(f"insert into customers (customer_id, first_name, last_name, phone_number, street, house_number, zipcode, city_id, is_member) values ( \
                        {customer_id}, \
                        '{fake.first_name()}', \
                        '{fake.last_name()}', \
                        '{fake.unique.basic_phone_number()}', \
                        '{fake.street_name()}', \
                        {random.randint(1,300)}, \
                        '{random.randint(10000,99999)}', \
                        '{random.randint(1,city_count)}', \
                        {is_member} \
                        )")
        
        if is_member == 1:
            cursor.execute(f"update customers set email = '{fake.unique.email()}' where customer_id = {customer_id}")
        
        customer_id += 1 

    # commit the changes
    db.commit()

def insert_products(db, product_count):
    # insert required number of products into the db

    # open a cursor
    cursor = db.cursor()

    # instantiate a faker
    fake = Faker()

    product_id = 1
    for _ in range(product_count):
        cursor.execute(f"insert into products (product_id, name, details, category_id, image_url) values ( \
                        {product_id}, \
                        '{" ".join(fake.words(random.randint(1,5)))} {fake.color_name()} {random.randint(1,10000)}', \
                        '{" ".join(fake.words(random.randint(1,20)))}', \
                        '{random.randint(1,10)}', \
                        '{fake.image_url()}' \
                        )")
        product_id += 1 

    # commit the changes
    db.commit()


def insert_vendors(db, vendor_count, city_count):
    # insert required number of vendors into the db

    # open a cursor
    cursor = db.cursor()

    # instantiate a faker
    fake = Faker()

    vendor_id = 1
    for _ in range(vendor_count):
        cursor.execute(f"insert into vendors (vendor_id, name, phone_number, street, house_number, zipcode, city_id, commission_rate) values ( \
                        {vendor_id}, \
                        '{fake.unique.name()}', \
                        '{fake.unique.basic_phone_number()}', \
                        '{fake.street_name()}', \
                        {random.randint(1,300)}, \
                        '{random.randint(10000,99999)}', \
                        '{random.randint(1,city_count)}', \
                        '{random.randint(1,10)/100}' \
                        )")
        
        vendor_id += 1 

    # commit the changes
    db.commit()


def insert_vendor_products(db, vendor_count, product_count):
    # insert required number of vendor_products into the db

    # open a cursor
    cursor = db.cursor()

    vendor_product_id = 1
    product_id = 1
    vendors_set = set()

    for _ in range(product_count):
        # each product can have 0 to 3 vendors
        vendor_product_count = random.randint(0,3)
        vendors_set.clear()
        for _ in range(vendor_product_count):
            # a unique vendor_id is assigned to each product in every iteration
            vendor_id = random.randint(1, vendor_count)
            while vendor_id in vendors_set:
                vendor_id = random.randint(1, vendor_count)
            vendors_set.add(vendor_id)
            cursor.execute(f"insert into vendor_products (vendor_product_id, vendor_id, product_id, price) values ( \
                            {vendor_product_id}, \
                            {vendor_id}, \
                            {product_id}, \
                            {random.randint(1,100)+random.random()} \
                            )")
        
            vendor_product_id += 1 

        product_id +=1

    # commit the changes
    db.commit()
    return vendor_product_id-1

def insert_orders(db, order_count, customer_count):
    # insert required number of orders into the db

    # open a cursor
    cursor = db.cursor()

    # instantiate a faker
    fake = Faker()

    order_id = 1
    for _ in range(order_count):
        try:
            created_at = fake.date_time_this_decade()
            cursor.execute(f"insert into orders (order_id, customer_id, total_cost, created_at) values ( \
                            {order_id}, \
                            {random.randint(1, customer_count)}, \
                            {random.randint(0,200)+random.random()}, \
                            '{created_at}' \
                            )")
        except:
            created_at = datetime.now()
            cursor.execute(f"insert into orders (order_id, customer_id, total_cost, created_at) values ( \
                            {order_id}, \
                            {random.randint(1, customer_count)}, \
                            {random.randint(0,200)+random.random()}, \
                            '{created_at}' \
                            )")

        order_id += 1 

    # commit the changes
    db.commit()


def insert_order_details(db, order_count, vendor_product_count):
    # insert required number of order_details into the db

    # open a cursor
    cursor = db.cursor()

    order_detail_id = 1
    order_id = 1    
    vendor_product_set = set()

    for _ in range(order_count):
        # each order can have 1 to 5 details
        order_detail_count = random.randint(1,5)
        vendor_product_set.clear()
        order_total_cost = 0
        for _ in range(order_detail_count):
            # a unique vendor_product_id is assigned to each order_detail in every iteration
            vendor_product_id = random.randint(1, vendor_product_count)
            while vendor_product_id in vendor_product_set:
                vendor_product_id = random.randint(1, vendor_product_count)
            vendor_product_set.add(vendor_product_id)

            # calculate order_total_cost to update it for most of the orders
            quantity = random.randint(1, 4)
            price = round(random.randint(1,100)+random.random(),2)
            order_total_cost += quantity * price

            cursor.execute(f"insert into order_details (order_detail_id, order_id, vendor_product_id, quantity, price) values ( \
                            {order_detail_id}, \
                            {order_id}, \
                            {vendor_product_id}, \
                            {quantity}, \
                            {price} \
                            )")
        
            order_detail_id += 1 

        # for about 80% of orders sum(quantity*price) of their details set to be equals to their total_cost
        if random.random() >= 0.2:
            cursor.execute(f"update orders set total_cost = {order_total_cost} where order_id = {order_id}")

        order_id +=1

    # commit the changes
    db.commit()


def insert_order_statuses(db, order_count):
    # insert required number of order_statuses into the db

    # open a cursor
    cursor = db.cursor()

    order_status_id = 1
    order_id = 1    

    for _ in range(order_count):
        # each order can have a level of first three statuses which means it has to pass all the statuses before that level
        order_status_level = random.randint(1,3)
        status_id = 1
        for _ in range(order_status_level):
            cursor.execute(f"insert into order_statuses (order_status_id, order_id, status_id, cost) values ( \
                            {order_status_id}, \
                            {order_id}, \
                            {status_id}, \
                            {round(random.randint(1,5)+random.random(),2)} \
                            )")
            status_id += 1
            order_status_id += 1 
        
        # if order is shipped it can have one of the statuses between 3 to 7. 50% of shipped orders set to have further steps after shipment
        if order_status_level == 3 and random.random() < 0.5:
            cursor.execute(f"insert into order_statuses (order_status_id, order_id, status_id, cost) values ( \
                {order_status_id}, \
                {order_id}, \
                {random.randint(4,7)}, \
                {round(random.randint(1,5)+random.random(),2)} \
                )")
            order_status_id += 1 

        order_id +=1

    # commit the changes
    db.commit()


def insert_reviews(db, review_count, customer_count, vendor_product_count):
    # insert required number of reviews into the db

    # open a cursor
    cursor = db.cursor()

    # instantiate a faker
    fake = Faker()

    customer_vendor_product_set = set()

    review_id = 1
    for _ in range(review_count):
        # a unique combination of customer_id and vendor_product_id is assigned to each review in every iteration
        customer_id = random.randint(1, customer_count)
        vendor_product_id = random.randint(1, vendor_product_count)
        customer_vendor_product_id = (customer_id,vendor_product_id)

        while customer_vendor_product_id in customer_vendor_product_set:
                customer_id = random.randint(1, customer_count)
                vendor_product_id = random.randint(1, vendor_product_count)
                customer_vendor_product_id = (customer_id,vendor_product_id)

        customer_vendor_product_set.add(customer_vendor_product_id)

        cursor.execute(f"insert into reviews (review_id, customer_id, vendor_product_id, description, rating) values ( \
                        {review_id}, \
                        {customer_id}, \
                        {vendor_product_id}, \
                        '{" ".join(fake.sentences())}', \
                        {random.randint(1,5)} \
                        )")
        
        review_id += 1 

    # commit the changes
    db.commit()


def insert_sessions(db, session_count, customer_count):
    # insert required number of orders into the db

    # open a cursor
    cursor = db.cursor()

    # instantiate a faker
    fake = Faker()

    session_id = 1
    for _ in range(session_count):
        try:
            start_at = fake.date_time_this_decade()
            cursor.execute(f"insert into sessions (session_id, customer_id, start_at) values ( \
                            {session_id}, \
                            {random.randint(1, customer_count)}, \
                            '{start_at}' \
                            )")
        except:
            start_at = datetime.now()
            cursor.execute(f"insert into sessions (session_id, customer_id, start_at) values ( \
                            {session_id}, \
                            {random.randint(1, customer_count)}, \
                            '{start_at}' \
                            )")

        # about 70% of sessions are ended
        if random.random() >= 0.3:
            try:
                end_at = start_at + timedelta(hours=random.randint(1,33))
                cursor.execute(f"update sessions set end_at = '{end_at}' where session_id = {session_id}")
            except:
                end_at = start_at + timedelta(minutes=5)
                cursor.execute(f"update sessions set end_at = '{end_at}' where session_id = {session_id}")

        session_id += 1 

    # commit the changes
    db.commit()


def main():
    # number of fake records for tables
    city_count = 100
    customer_count = 10000
    product_count = 10000
    vendor_count = 1000
    order_count = 100000
    review_count = 5000
    session_count = 1000000

    # open db connection
    db = connect_db()

    # delete all pre-inserted records
    delete_all(db)

    # insert fake records into tables
    insert_categories(db)
    insert_statuses(db)
    insert_cities(db, city_count)
    insert_customers(db, customer_count, city_count)
    insert_products(db, product_count)
    insert_vendors(db, vendor_count, city_count)
    vendor_product_count = insert_vendor_products(db, vendor_count, product_count)
    insert_orders(db, order_count, customer_count)
    insert_order_details(db, order_count, vendor_product_count)
    insert_order_statuses(db, order_count)
    insert_reviews(db, review_count, customer_count, vendor_product_count)
    insert_sessions(db, session_count, customer_count)

    # close the db connection
    disconnect_db(db)

if __name__ == "__main__":
    main()