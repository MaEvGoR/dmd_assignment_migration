


def query_one(mongo_client):
    '''
    Retrieve all the customers that rented movies of at least two different categories during the current year*.
(*) The current year is the year of the most recent records in the table rental.
    '''

    pl = [
        {
            "$project":
                {
                    "customer_id": "$customer_id",
                    "year":
                        {
                            "$year":
                                {
                                    "$dateFromString":
                                        {
                                            "dateString": "$rental_date"
                                        }
                                }
                        },
                    "inventory_id": "$inventory_id"
                }
        },
        {
            "$sort":
                {
                    "year": -1
                }
        },
        {
            "$group":
                {
                    "_id": "$customer_id",
                    "count":
                        {
                            "$sum": 1
                        },
                    "inv_ids":
                        {
                            "$push":
                                {
                                    "inv_id": "$inventory_id",
                                    "year": "$year"
                                }
                        }
                }
        },
        {
            "$match":
                {
                    "count":
                        {
                            "$gt": 2
                        },
                }
        },
    ]

    rentals = list(mongo_client['rental'].aggregate(pipeline=pl))

    # Format:
    #     _id: customer id
    #     count: # of rentals
    #     in_ids: list of
    #                     inventory item id
    #                     rental year

    fit_customer_ids = []

    for rental in rentals:
        current_set_of_categories = set()

        current_customer_id = rental['_id']
        current_max_year = 2006
        current_year_inv_ids = []

        for item in rental['inv_ids']:
            year = item['year']
            if year == current_max_year:
                current_year_inv_ids.append(item['inv_id'])

        if len(current_year_inv_ids) < 2:
            continue

        pl = [
            {
                '$lookup':
                    {
                        'from': 'film_category',
                        'localField': 'film_id',
                        'foreignField': 'film_id',
                        'as': 'categories'
                    }
            },
            {
                '$match':
                    {
                        'inventory_id':
                            {
                                '$in': current_year_inv_ids
                            }
                    }
            },
            {
                "$project":
                    {
                        "film_id": "$film_id",
                        '_id': False,
                        'category':
                            {
                                '$arrayElemAt': ['$categories', 0]
                            }
                    }
            },
            {
                '$project':
                    {
                        "category": "$category.category_id"
                    }
            }
        ]

        categories = list(mongo_client['inventory'].aggregate(pipeline=pl))

        for category in categories:
            current_set_of_categories.add(category['category'])
            if len(current_set_of_categories) > 1:
                fit_customer_ids.append(current_customer_id)
                break

    pl = [
        {
            '$project':
                {
                    'customer_id': True,
                    'first_name': True,
                    'last_name': True,
                    '_id': False
                }
        },
        {
            '$match':
                {
                    'customer_id':
                        {
                            '$in': fit_customer_ids
                        }
                }
        },
    ]
    names_raw = list(mongo_client['customer'].aggregate(pipeline=pl))
    names = ["{} {}".format(name_raw['first_name'], name_raw['last_name']) for name_raw in names_raw]

    return names