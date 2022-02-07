class LineInfo:
    types = {
        'Clinch point': 'str',
        'Curve_Result': 'ok',
        'EndForce_Value [N]': 'float',
        'EndForce_LoLim': 'float',
        'EndForce_UpLim': 'float',
        'EndPosition_Value': 'float',
        'EndPosition_LoLim': 'float',
        'EndPosition_UpLim': 'float',
        'Gradient_Result': 'ok',
    }
    
    def __init__(self, line):
        splited = line.split(';')
        self.title = splited[0]
        self.values = None
        values = splited[1:-1]
        try:
            self.__parse_and_set_values(values)
        except:
            pass
        
    def __parse_and_set_values(self, values):
        value_type = LineInfo.types[self.title]
        if value_type == 'ok':
            self.values = list(map(lambda x: True if x == 'OK' else False, values))
        elif value_type == 'int':
            self.values = list(map(int, values))
        elif value_type == 'float':
            self.values = list(map(lambda x: float(x.replace(',', '.')), values))
        elif value_type == 'str':
            self.values = values
        else:
            raise '{0}타입은 없습니다.'.format(value_type)
