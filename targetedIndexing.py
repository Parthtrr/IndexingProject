from targeted_indexing import targeted_index

def main():
    ticker = input("Enter Ticker: ")
    start_date = input("Enter Start Date (YYYY-MM-DD): ")
    end_date = input("Enter End Date (YYYY-MM-DD): ")
    targeted_index(ticker, start_date, end_date)

if __name__ == "__main__":
    main()