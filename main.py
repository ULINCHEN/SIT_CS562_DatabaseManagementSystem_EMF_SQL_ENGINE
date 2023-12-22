import dbconnection
import phiparser
import mfquerybuilder
import INPUT_STRING

testString = INPUT_STRING.INPUT_STRING()


def main(input, address):

    # create an instance of Parser Object
    parser = phiparser.PhiParser()
    # pass the EMF query string into parser
    parser.parse_input_string(input)

    # create an instance of query builder and pass the parser.phi_dict, which contains
    # the EMF query data structure after parser process it
    builder = mfquerybuilder.MfQueryBuilder(parser.phi_dict)
    # use builder.run() to start building the python file inside builder instance
    builder.run()

    # output the file to target address
    builder.create_python_file(address)


if __name__ == '__main__':
    main(testString.ESQL_ONE, "./output/output_1.py")
    main(testString.ESQL_TWO, "./output/output_2.py")
    main(testString.ESQL_THREE, "./output/output_3.py")

