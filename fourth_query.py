

def query_four(mongo_client):
    pl = [
        {
            '$lookup':
                {
                    'from': 'inventory',
                    'localField': 'inventory_id',
                    'foreignField': 'inventory_id',
                    'as': 'film'
                }
        },
        {
            '$project':
                {
                    'inventory_id': True,
                    'customer_id': True,
                    'film':
                        {
                            '$arrayElemAt': ['$film', 0]
                        },
                    '_id': False
                }
        },
        {
            '$project':
                {
                    'inventory_id': True,
                    'customer_id': True,
                    'film_id': '$film.film_id',
                    '_id': False
                }
        },
        {
            '$lookup':
                {
                    'from': 'film',
                    'localField': 'film_id',
                    'foreignField': 'film_id',
                    'as': 'film_info'
                }
        },
        {
            '$project':
                {
                    'inventory_id': True,
                    'customer_id': True,
                    'film_id': True,
                    'film_info':
                        {
                            '$arrayElemAt': ['$film_info', 0]
                        }
                }
        },
        {
            '$project':
                {
                    'inventory_id': True,
                    'customer_id': True,
                    'film_id': True,
                    'film_title': '$film_info.title'
                }
        }
    ]

    films = list(mongo_client['rental'].aggregate(pipeline=pl))
    # {'inventory_id': 1525, 'customer_id': 459, 'film_id': 333, 'film_title': 'Freaky Pocus'}

    pl = [
        {
            '$lookup':
                {
                    'from': 'rental',
                    'localField': 'customer_id',
                    'foreignField': 'customer_id',
                    'as': 'rentals'
                }
        },
        {
            '$project':
                {
                    'customer_id': True,
                    'first_name': True,
                    'last_name': True,
                    'rentals':
                        {
                            '$map':
                                {
                                    'input': '$rentals',
                                    'as': 'rental',
                                    'in': '$$rental.inventory_id'
                                }
                        },
                    '_id': False
                }
        }
    ]

    customers = list(mongo_client['customer'].aggregate(pipeline=pl))
    # {'customer_id': 524, 'first_name': 'Jared', 'last_name': 'Ely', 'rentals': <list of inventory_ids>}

    for customer in customers:
        customer['watched_films'] = set()
        customer['film_id-title'] = {}

    for rental_film in films:
        for customer in customers:
            if customer['customer_id'] == rental_film['customer_id']:
                customer['watched_films'].add(rental_film['film_id'])
                customer['film_id-title'][rental_film['film_id']] = rental_film['film_title']

    # {'customer_id': 524, 'first_name': 'Jared', 'last_name': 'Ely', 'rentals': <list of inventory_ids>,
    # 'watched_films': set(), 'film_id-title':{'film_id':'title', ...}}
    output = []
    for customer_in in range(len(customers)):
        suggested_films = {}
        for compare_in in range(len(customers)):
            fit_rate = 1 - len(customers[customer_in]['watched_films'] - customers[compare_in]['watched_films']) / (
                len(customers[customer_in]['watched_films']))

            probably_suggested_films = customers[compare_in]['watched_films'] - customers[customer_in]['watched_films']
            for p_suggest_film in probably_suggested_films:
                if p_suggest_film in list(suggested_films.keys()):
                    suggested_films[p_suggest_film]['fit_rate'] += fit_rate
                    suggested_films[p_suggest_film]['fit_rate'] /= 2
                else:
                    suggested_films[p_suggest_film] = {'fit_rate': fit_rate,
                                                       'title': customers[compare_in]['film_id-title'][p_suggest_film]}

        suggest = []

        full_name = '{} {}'.format(customers[customer_in]['first_name'], customers[customer_in]['last_name'])

        for suggest_film in suggested_films.keys():
            suggest.append(
                [suggested_films[suggest_film]['title'], int(suggested_films[suggest_film]['fit_rate'] * 100)])

        suggest.sort(key=lambda a: a[1], reverse=True)
        output.append([full_name, suggest[:10]])

    return output