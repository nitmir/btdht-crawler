ORDER_BY_SCORE = '1'
ORDER_BY_NAME = '2'
ORDER_BY_SIZE = '3'
ORDER_BY_CREATED = '4'
ORDER_BY_FILES = '5'
ORDER_BY_PEERS = '6'
ORDER_BY_SEEDS = '7'

categories = [
    'video',
    'audio',
    'archive',
    'document',
    'software',
    'image',
    'other',
]


categories_colors = [
    '5c6bc0',
    '7e57c2',
    '9ccc65',
    'ffca28',
    'd4e157',
    'a1887f',
    '78909c',
]

categories_choices = [(i, cat.title()) for (i, cat) in enumerate(['all'] + categories)]
