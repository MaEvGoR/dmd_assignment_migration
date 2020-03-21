



def query_two(mongo_client):
    pl_actors = [
        {
            "$group":
                {
                    '_id': '$actor_id',
                    'films':
                        {
                            '$push': '$film_id'
                        }
                }
        },
        {
            '$lookup':
                {
                    'from': 'actor',
                    'localField': '_id',
                    'foreignField': 'actor_id',
                    'as': 'name'
                }
        },
    ]

    pl_films = [
        {
            '$group':
                {
                    '_id': '$film_id',
                    'actors':
                        {
                            '$push': '$actor_id'
                        }
                }
        }
    ]

    actors = list(mongo_client['film_actor'].aggregate(pipeline=pl_actors))
    films = list(mongo_client['film_actor'].aggregate(pipeline=pl_films))

    output = []
    for actor_one_i in range(len(actors)):
        for actor_two_i in range(actor_one_i + 1, len(actors)):
            count = 0
            for film in films:
                if actors[actor_one_i]['_id'] in film['actors'] and actors[actor_two_i]['_id'] in film['actors']:
                    count += 1
            output.append(['{} {} {}'.format(actors[actor_one_i]['name'][0]['first_name'],
                                             actors[actor_one_i]['name'][0]['last_name'], actors[actor_one_i]['_id']),
                           '{} {} {}'.format(actors[actor_two_i]['name'][0]['first_name'],
                                             actors[actor_two_i]['name'][0]['last_name'], actors[actor_two_i]['_id']),
                           count])

    return output