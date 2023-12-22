# define the data structure for store phi operation:
import re
from pyparsing import Word, nums, alphas, Literal, Group, ZeroOrMore, Suppress


def print_obj(key, value):
    if key == "null":
        print("Non-aggregate: ")
        for i in value:
            print("value:" + i)
    else:
        for obj in value:
            obj.log()


class PhiParser:

    # Define MF_Structure
    def __init__(self):
        # print("------ MFSQL ENGINE V 1.0 ------")
        # print("------ Author: YouLinChen, CWID: 20012293, E-mail: ychen10@stevens.edu ------")
        print("****** Start init PhiParser ******")
        self.phi_dict = {}
        self.phi_dict["SELECT_ATTRIBUTE_s"] = {}
        self.phi_dict["NUMBER_OF_GROUPING_VARIABLES_n"]: int
        self.phi_dict["GROUPING_ATTRIBUTES_v"] = []
        self.phi_dict["F - VECT_f"] = {}
        self.phi_dict["SELECT_CONDITION"] = {}

    def parse_input_string(self, input_string):

        # ------ regexp ------

        # Define the regular expression patterns
        patterns = {
            "SELECT ATTRIBUTE(S):": r"SELECT ATTRIBUTE\(S\):\n(.*?)\n",
            "NUMBER OF GROUPING VARIABLES(n):": r"NUMBER OF GROUPING VARIABLES\(n\):\n(.*?)\n",
            "GROUPING ATTRIBUTES(V):": r"GROUPING ATTRIBUTES\(V\):\n(.*?)\n",
            "F - VECT([F]):": r"F - VECT\(\[F\]\):\n(.*?)\n",
            "SELECT CONDITION - VECT([σ]):": r"SELECT CONDITION - VECT\(\[σ\]\):\n((?:.*\n)*)",
        }

        # Search for each pattern in the string and extract the matched part
        for key, pattern in patterns.items():
            match = re.search(pattern, input_string, re.DOTALL)
            if match:
                result = match.group(1).strip().split('\n')

                # Split each line by commas, except for the conditions
                match key:

                    case "SELECT ATTRIBUTE(S):":
                        for item in result:
                            item = item.strip()
                            if len(item) != 0:
                                self.muti_varible_parse(item, "SELECT_ATTRIBUTE_s")

                    case "NUMBER OF GROUPING VARIABLES(n):":
                        self.phi_dict["NUMBER_OF_GROUPING_VARIABLES_n"] = int(result[0])

                    case "GROUPING ATTRIBUTES(V):":
                        res = result[0].split(",")
                        for item in res:
                            item = item.strip()
                            self.phi_dict["GROUPING_ATTRIBUTES_v"].append(item)

                    case "F - VECT([F]):":
                        for item in result:
                            item = item.strip()
                            if len(item) != 0:
                                self.muti_varible_parse(item, "F - VECT_f")

                    case "SELECT CONDITION - VECT([σ]):":
                        for item in result:
                            item = item.strip()
                            if len(item) != 0:
                                self.muti_varible_parse(item, "SELECT_CONDITION")
        print(" ------ Well done! Input has been parse into MF structure! ------ ")

    # ------ methods for parse each operation ------
    def muti_varible_parse(self, string, typeName):

        arr = string.strip().split(',')
        for s in arr:
            s = s.strip()
            # check if string start with Non digits, if not then it must be non-aggregate select attribute
            if s[0].isdigit():

                temp = s.split('_')

                operation = None

                if typeName == "SELECT_ATTRIBUTE_s":
                    operation = Unit_operation(temp[0], temp[1], temp[2])

                elif typeName == "F - VECT_f":
                    operation = Unit_operation(temp[0], temp[1], temp[2])

                elif typeName == "SELECT_CONDITION":

                    pattern = r'(\d+)\.(\w+)\s*([<>!=]+)\s*\'([A-Za-z]+)\''


                    match = re.match(pattern, s)

                    if match:

                        number = int(match.group(1))
                        target = match.group(2)
                        sign = match.group(3)
                        condition = match.group(4)

                        operation = Unit_condition()
                        operation.number = number
                        operation.sign = sign
                        operation.target = target
                        operation.condition = condition

                self.phi_dict[typeName].setdefault(s[0], []).append(operation)

            else:
                self.phi_dict[typeName].setdefault("null", []).append(s)

    def show_dict(self):
        print("------ Current MF Structure show as below ------")
        for key, value in self.phi_dict.items():
            match key:
                case "SELECT_ATTRIBUTE_s":
                    print("------ SELECT_ATTRIBUTE_s ------")
                    for k, v in value.items():
                        print_obj(k, v)

                case "NUMBER_OF_GROUPING_VARIABLES_n":
                    print("------ NUMBER_OF_GROUPING_VARIABLES_n ------")
                    print(self.phi_dict["NUMBER_OF_GROUPING_VARIABLES_n"])

                case "GROUPING_ATTRIBUTES_v":
                    print("------ GROUPING_ATTRIBUTES_v ------")
                    for i in value:
                        print(i)

                case "F - VECT_f":
                    print("------ F - VECT_f ------")
                    for k, v in value.items():
                        print_obj(k, v)

                case "SELECT_CONDITION":
                    print("------ SELECT_CONDITION ------")
                    for k, v in value.items():
                        print_obj(k, v)


# ------ Use for store grouping attributes ------
class Unit_operation:
    def __init__(self, number, agg, target):
        self.number = number
        self.agg = agg
        self.target = target
        self.value = 0

    def log(self):
        print("number: " + str(self.number))
        print("aggregate: " + self.agg)
        print("target: " + self.target)
        print("Current Value: " + str(self.value))


# ------ Use for store select condition ------
class Unit_condition:
    def __int__(self, number, target, sign, condition):
        self.number = number
        self.target = target
        self.sign = sign
        self.condition = condition

    def log(self):
        print("number: " + str(self.number))
        print("target: " + self.target)
        print("sign: " + self.sign)
        print("condition: " + self.condition)


# input_string = '''
#         SELECT ATTRIBUTE(S):
#         cust
#         NUMBER OF GROUPING VARIABLES(n):
#         1
#         GROUPING ATTRIBUTES(V):
#         cust
#         F - VECT([F]):
#         1_sum_quant
#         SELECT CONDITION - VECT([σ]):
#         1.state = 'NY'
#         '''
#
# parser = PhiParser()
# parser.parse_input_string(input_string)
#
# # 打印解析结果
#parser.show_dict()
