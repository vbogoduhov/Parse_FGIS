class TypeParseSi:
    __LST_SEP = [' ', '-', '/', '.']
    def __init__(self, type_from_card, type_from_file):
        self.type_from_card = type_from_card
        self.original_type = type_from_file
        self.split_type()

    def split_type(self):
        self.dict_split = {'separator': {sep: {} for sep in TypeParseSi.__LST_SEP}}

        for sep in TypeParseSi.__LST_SEP:
            self.dict_split['separator'][sep]['value'] = self.original_type.split(sep=sep)
            self.dict_split['separator'][sep]['type_value'] = type(self.dict_split['separator'][sep]['value'])
            self.dict_split['separator'][sep]['lenght_value'] = len(self.dict_split['separator'][sep]['value'])

        separatop, max_len = self.__get_max_len(self.dict_split)
        self.dict_split['max_lenght'] = {'separator': separatop,
                                 'lenght': max_len}

    def __get_max_len(self, dct):
        tmp = {sep: dct['separator'][sep]['lenght_value'] for sep in TypeParseSi.__LST_SEP}
        key, max = None, 0
        for item in tmp.items():
            if item[1] > max:
                max = item[1]
                key = item[0]
        return  key, max

    def search_match(self, pattern, text):
        return True if text.find(pattern) != -1 else False

    def parse(self):
        dict_result_parse = {}
        if self.dict_split != None:
            sep = self.dict_split['max_lenght']['separator']
            lst = self.dict_split['separator'][sep]['value']
            for i in lst:
                if self.search_match(i, self.type_from_card):
                    dict_result_parse[i] = True
                else:
                    dict_result_parse[i] = False
            tmp_pattern = " ".join(lst) if sep == "-" else "-".join(lst)
            dict_result_parse[tmp_pattern] = True if self.search_match(tmp_pattern, self.type_from_card) else False
            dict_result_parse[self.original_type] = True if self.search_match(self.original_type, self.type_from_card) else False

        return dict_result_parse



def main():
    pass


if __name__ == '__main__':
    main()