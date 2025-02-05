from full_indexing import full_index
from incremental_indexing import incremental_index
from targeted_indexing import targeted_index

def main():
    print("1. Full Indexing")
    print("2. Incremental Indexing")
    print("3. Targeted Indexing")
    choice = input("Choose the indexing type (1/2/3): ")

    if choice == "1":
        full_index()
    elif choice == "2":
        incremental_index()
    elif choice == "3":
        ticker = input("Enter Ticker: ")
        start_date = input("Enter Start Date (YYYY-MM-DD): ")
        end_date = input("Enter End Date (YYYY-MM-DD): ")
        targeted_index(ticker, start_date, end_date)
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()