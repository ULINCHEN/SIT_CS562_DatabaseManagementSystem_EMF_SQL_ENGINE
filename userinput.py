class UserInput:

    def __init__(self):
        self.inputBody = ""

    def getPhiInputAll(self):
        data = input("Please copy your entire query in phi_format below: \n")
        self.inputBody += data
        return self.inputBody

    def getPhiInputLineByLine(self):
        selectAttr = input("SELECT ATTRIBUTE(S):\n")
        numberOfGrouping = input("NUMBER OF GROUPING VARIABLES(n):\n")
        groupingAttr = input("GROUPING ATTRIBUTES(V):\n")
        fVect = input("F - VECT([F]):\n")
        selectCondition = input("SELECT CONDITION - VECT([σ]):\n")
        self.inputBody = f"""
SELECT ATTRIBUTE(S):
{selectAttr}
NUMBER OF GROUPING VARIABLES(n):
{numberOfGrouping}
GROUPING ATTRIBUTES(V):
{groupingAttr}
F - VECT([F]):
{fVect}
SELECT CONDITION - VECT([σ]):
{selectCondition}
        """


if __name__ == '__main__':
    temp = UserInput()
    temp.getPhiInputLineByLine()
    print(temp.inputBody)