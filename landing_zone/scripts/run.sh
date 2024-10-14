#!/bin/bash

echo "Welcome to our BDM Lab 1 Solution!"

while true; do
    echo ""
    echo "Please choose an option:"
    echo "1) Install necessary packages"
    echo "2) Load data into the temporal landing zone"
    echo "3) Move data from the temporal to the persistent zone"
    echo "0) Exit"

    echo ""

    read -r option

    case $option in
        1)
            echo "Installing necessary packages..."
            pip install -r requirements.txt
            ;;
        2)
            echo "Loading data into the temporal landing zone..."
            python dataCollectors.py
            ;;
        3)
            echo "Moving data from the temporal to the persistent zone..."
            python hdfsToHBase.py
            ;;
        0)
            echo "Exiting script."
            exit 0
            ;;
        *)
            echo "Invalid option. Please choose a valid option."
            ;;
    esac
done
