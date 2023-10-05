# Makefile for a pipeline running timestone.py, gzipping a directory, and then uploading the resulting file to a ec2 server.
# This makefile condenses the following steps into one set of calls.
# You can run the full pipeline with 'make all filename='<YourFileName>.zip''
# Alternatively, you can execute each step individually.

# Usage:
# make <target> filename='<YourFileName>.zip'
# Targets:
# all        Run prep, gzip, and upload commands all in order
# prep       Process the data file using timestone
# gzip       Create a gzip of the directory, calculate and print the difference in size
# upload     Upload the gzipped file to the ec2 server and send a notification after successful upload


# Define the target names
.PHONY: all prep gzip upload help setup monitor

# ANSI escape codes for colorful echo output
GREEN := "\e[1;32m"
YELLOW := "\e[1;33m"
CLEAR := "\e[0m"

# DATE: Extracts the date from the filename
DATE := $(shell basename $(filename) .zip | cut -d'_' -f4,5)
# DIR: defines the directory path based on the extracted DATE
DIR:=./data/Stage3-combined_and_ready_for_upload/$(DATE)

help:
	@echo "Usage:"
	@echo "  make <target> filename='<YourFileName>.zip'"
	@echo ""
	@echo "Targets:"
	@echo "  all        Run prep, gzip, and upload commands all in order"
	@echo "  prep       Process the data file using timestone"
	@echo "  gzip       Create a gzip of the directory, calculate and print the difference in size"
	@echo "  upload     Upload the gzipped file to the ec2 server and send a notification after successful upload"
	@echo "  setup      Set up the folder structure and .env file"
	@echo "  help       Show this help message"
	@echo "  monitor    Monitor the upload to timestream"

all:
	@printf $(GREEN)"\nStarting the prep command..."$(CLEAR)
	make prep filename=$(filename)
	@printf $(YELLOW)"\nStarting the GZip Compression..."$(CLEAR)
	make gzip filename=$(filename)
	@printf $(GREEN)"\nStarting the upload command..."$(CLEAR)
	make upload filename=$(filename)
	@printf $(YELLOW)"\nAll commands executed successfully!"$(CLEAR)

setup:
	@printf $(YELLOW)"\nSetting up the environment..."$(CLEAR) && \
	mkdir -p data/zips && \
	mkdir -p data/Stage3-combined_and_ready_for_upload && \
	mkdir -p data/Stage2-deduped_eda_cleaned && \
	mkdir -p data/unzipped && \
	mkdir -p logs && \
	touch .env && \
	printf $(YELLOW)"\nDon't forget to add a slack api endpoint to your env file if you'd like to use those alerts.:"$(CLEAR) && \
	printf $(YELLOW)"\nSLACK_WEBHOOK_URL='https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXX/XXXXXXXXXXXXXXXXXXXXXXXX'"$(CLEAR) && \
	printf $(YELLOW)"\nYou'll also still need to create your environment and install the requirements:"$(CLEAR)

# Processes the data file using timestone
prep:
	@printf $(YELLOW)"\nProcessing the data file using timestone..."$(CLEAR)
	python timestone.py --prep --path='data/zips/$(filename)' --output='./data' -as --verbose

# Creates a gzip of the directory, calculates and prints the difference in size
gzip:
	@orig_size_kb=`du -sk $(DIR) | awk '{print $$1}'` && \
	orig_size_gb=`echo "scale=2; $$orig_size_kb / 1024 / 1024" | bc` && \
	tar -czf "$(DIR).tar.gz" $(DIR) && \
	gz_size_kb=`du -sk "$(DIR).tar.gz" | awk '{print $$1}'` && \
	gz_size_gb=`echo "scale=2; $$gz_size_kb / 1024 / 1024" | bc` && \
	size_diff_gb=`echo "scale=2; $$orig_size_gb - $$gz_size_gb" | bc` && \
	size_diff_pcent=`echo "scale=2; ($$size_diff_gb / $$orig_size_gb) * 100" | bc` && \
	printf $(GREEN)"\nSize difference: $$size_diff_gb GB\n"$(CLEAR) && \
    printf $(GREEN)"\nPercent difference: $$size_diff_pcent %%\n"$(CLEAR)

# Uploads the gzipped file to the ec2 server and sends a notification after successful upload
upload:
	@printf $(YELLOW)"\nUploading the gzipped file to the ec2 server and sending a notification after successful upload..."$(CLEAR) && \
	scp -i ~/.ssh/atheneum-production $(DIR).tar.gz ec2-user@ec2-54-82-45-59.compute-1.amazonaws.com: && \
	python -c "from file_handler import send_slack_notification; send_slack_notification('upload to ec2 complete')"

monitor:
	@printf $(YELLOW)"\nMonitoring the upload to timestream..."$(CLEAR) && \
	while sleep 5; do tasks=$$(aws timestream-write list-batch-load-tasks --query 'BatchLoadTasks[?TaskStatus!=`SUCCEEDED` && TaskStatus!=`FAILED`].[TaskId,TableName,TaskStatus]' --output text --profile=nocklab); if [ -z "$$tasks" ]; then printf $(GREEN)"\n No Tasks still running!\n" $(CLEAR); break; else printf $(YELLOW)"\nTasks still running:\n$$tasks\n" $(CLEAR); fi; done && \
	python -c "from file_handler import send_slack_notification; send_slack_notification('Upload to timestream complete')"
