


def query_three(mongo_client):
    pl = [
        {
            "$lookup":
                {
                    'from': 'rental',
                    'localField': 'inventory_id',
                    'foreignField': 'inventory_id',
                    'as': 'rental_records'
                }
        },
        {
            '$project':
                {
                    '_id': True,
                    'film_id': True,
                    'inventory_id': True,
                    'rental_records': True
                }
        },
        {
            '$group':
                {
                    '_id': '$film_id',
                    'rental_records_full':
                        {
                            '$push':
                                {
                                    'set_record': '$rental_records'
                                }
                        }
                }
        },
        {
            '$lookup':
                {
                    'from': 'film',
                    'localField': '_id',
                    'foreignField': 'film_id',
                    'as': 'film_info'
                }
        }
    ]

    pl_category = [
        {
            '$lookup':
                {
                    'from': 'category',
                    'localField': 'category_id',
                    'foreignField': 'category_id',
                    'as': 'category_name'
                }
        }
    ]

    result = list(mongo_client['inventory'].aggregate(pipeline=pl))
    categories = list(mongo_client['film_category'].aggregate(pipeline=pl_category))

    output = []

    for film in result:
        film_title = film['film_info'][0]['title']
        film_category = None
        for category_record in categories:
            if category_record['film_id'] == film['_id']:
                film_category = category_record['category_name'][0]['name']
                break

        required_number = 0
        for rental_record in film['rental_records_full']:
            required_number += len(rental_record['set_record'])

        output.append([film_title, film_category, required_number])

    return output