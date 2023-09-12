from constants import COMMAND_ALL_ROADS, COMMAND_QUIT, COMMAND_ROAD_BY_ID
from mq_manager import MQManager


class Client:
    def __init__(self) -> None:
        self.mq_manager = MQManager()
        self.mq_manager.add_queue('client')
        self.mq_manager.add_queue('manager')
        
    def run_console(self):
        while True:
            print(f'You can enter two types of requests:\n1. Get road points by road id, type: 1 <road_id>\n2. Get all road ids, type: 2\n3. Exit program: q')
            request_text = input().split()
            try:
                if request_text[0] == '1':
                    self.get_road_by_id(request_text[1])
                elif request_text[0] == '2':
                    self.get_all_roads()
                elif request_text[0] == 'q':
                    print('Exiting')
                    self.mq_manager.send_message('manager', {'command': COMMAND_QUIT, 'data': None})
                    return
                else:
                    print('Unknown request, please try again')
            except IndexError:
                print('Unknown request, please try again')
        
    def get_road_by_id(self, road_id: int):
        self.mq_manager.send_message('manager', {'command': COMMAND_ROAD_BY_ID, 'data': road_id})
        data = self.mq_manager.get_message('client')
        if not data['data']:
            print('Incorrect road id')
        else:
            print(f'ROAD ID {road_id}\n' + '\n'.join(map(str, data['data'])))
    
    def get_all_roads(self):
        self.mq_manager.send_message('manager', {'command': COMMAND_ALL_ROADS, 'data': None})
        data = self.mq_manager.get_message('client')
        print("All roads:", data['data'])
    