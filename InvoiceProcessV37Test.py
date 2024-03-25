import configparser
import logging
import imaplib
import ftplib
import regex
import queue
import json
import time
import re
import io
import os
import tkinter as tk
import numpy as np
import datetime
from typing import Tuple
from email import message_from_string
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.ai.formrecognizer import DocumentField
from azure.core.credentials import AzureKeyCredential
from threading import Thread
# ---------------------
# Configuration and Utilities Section
# ---------------------
class ConfigurationLoader:
    @staticmethod
    def load_config(filepath='config.ini'):
        try:
            config = configparser.ConfigParser()
            config.read(filepath)
            return config['DEFAULT']
        except FileNotFoundError:
            logging.getLogger('debug').critical("Configuration file not found. Please ensure config.ini is in the correct path.")
            raise
        except Exception as e:
            logging.getLogger('debug').critical(f"An error occurred while loading the configuration: {e}")
            raise

    @staticmethod
    def load_json(filepath):
        try:
            with open(filepath, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            logging.getLogger('debug').critical(f"JSON file not found at path: {filepath}")
            raise
        except json.JSONDecodeError:
            logging.getLogger('debug').critical(f"Error parsing JSON file at path: {filepath}")
            raise

    @staticmethod
    def load_business_subject_criteria():
        config_file='config.ini'
        config = ConfigurationLoader.load_config(config_file)
        return ConfigurationLoader.load_json(config["path_one"])

    @staticmethod
    def load_business_models_map():
        config_file='config.ini'
        config = ConfigurationLoader.load_config(config_file)
        return ConfigurationLoader.load_json(config["path_two"])

    @staticmethod
    def load_debtor_map():
        config_file='config.ini'
        config = ConfigurationLoader.load_config(config_file)
        return ConfigurationLoader.load_json(config["path_three"])

    @staticmethod
    def load_country_abbreviations_map():
        config_file='config.ini'
        config = ConfigurationLoader.load_config(config_file)
        return ConfigurationLoader.load_json(config["path_four"])

    @staticmethod
    def load_tax_qualifier_map():
        config_file='config.ini'
        config = ConfigurationLoader.load_config(config_file)
        return ConfigurationLoader.load_json(config["path_five"])

    @staticmethod
    def load_EU_country_abbreviations_map():
        config_file='config.ini'
        config = ConfigurationLoader.load_config(config_file)
        return ConfigurationLoader.load_json(config["path_seven"])
    
    @staticmethod
    def load_unit_map():
        config_file='config.ini'
        config = ConfigurationLoader.load_config(config_file)
        return ConfigurationLoader.load_json(config["path_eigth"])

class LoggingManager:
    @staticmethod
    def setup_logging():
        logger_prod = logging.getLogger('production')
        logger_prod.setLevel(logging.INFO)
        handler_prod = logging.FileHandler('INVOproduction.log', mode='a')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler_prod.setFormatter(formatter)
        logger_prod.addHandler(handler_prod)

        logger_debug = logging.getLogger('debug')
        logger_debug.setLevel(logging.DEBUG)
        handler_debug = logging.FileHandler('INVOdebug.log', mode='a')
        handler_debug.setFormatter(formatter)
        logger_debug.addHandler(handler_debug)

        logging.getLogger('azure').setLevel(logging.WARNING)
        logging.getLogger('debug').info("Logging setup complete")
# ----------------
# Test Environment Utilities
# ----------------
class TestEnvironmentUtilities:
    @staticmethod
    def save_xml_locally_for_test(key_value_pairs, cleaned_content):
        # Ensure this code is run only in a test environment
        idocs_dir = 'IDOCs'
        os.makedirs(idocs_dir, exist_ok=True)  # Ensure the IDOCs directory exists
        Creditor_number = key_value_pairs.get('Creditor_number')
        Debtor_international_location_number = key_value_pairs.get('Debtor_international_location_number')
        Invoice_number = key_value_pairs.get('Invoice_number')
        local_filename = f"{idocs_dir}/{Creditor_number}.{Debtor_international_location_number}.{Invoice_number}.xml"
        
        with open(local_filename, 'w', encoding='utf-8') as local_file:
            local_file.write(cleaned_content)
        logging.getLogger('production').info(f"Populated XML file saved locally to {local_filename} for testing purposes.")
# ----------------
# GUI Section
# ----------------
class EmailQueueWindow(tk.Tk):
    def __init__(self, email_queue, email_scanner):
        super().__init__()
        self.email_queue = email_queue
        self.email_scanner = email_scanner
        self.title("Email Queue")
        self.geometry("1200x650")
        self.queue_display = tk.Text(self, height=37, width=140)
        self.queue_display.pack()
        self.update_queue_display()

        refresh_button = tk.Button(self, text="Refresh", command=self.refresh)
        refresh_button.pack(pady=(11, 0))

    def refresh(self):
        self.email_scanner.scan_emails()

    def update_queue_display(self):
        self.queue_display.delete(1.0, tk.END)
        current_time = time.time()
        for i, item in enumerate(list(self.email_queue.queue)):
            msg_number, email_message, timestamp = item
            time_in_queue = current_time - timestamp
            self.queue_display.insert(tk.END, f"{i+1}. Email Number: {msg_number}, Subject: {email_message['Subject']}, Time in Queue: {np.round(time_in_queue)}\n")
        self.after(1000, self.update_queue_display) 
# ------------------------
# Email Processing Section
# ------------------------
class EmailScannerService:
    def __init__(self, config_file='config.ini', check_interval=600):
        self.workflow = InvoiceWorkflowManager(config_file)
        self.check_interval = check_interval
        self.business_subject_criteria = ConfigurationLoader.load_business_subject_criteria()
        self._denied_emails_count = 0
        self.email_queue = queue.Queue()
        self.email_processor = EmailProcessorService(self.workflow, self.email_queue, self.business_subject_criteria)

    def _add_to_queue(self, msg_number, email_message):
        timestamp = time.time()
        self.email_queue.put((msg_number, email_message, timestamp))

    def start_gui(self):
        app = EmailQueueWindow(self.email_queue, self)
        app.mainloop()

    def scan_emails(self):
        logging.getLogger('production').info("Starting email scan...")
        try:
            imap_server = imaplib.IMAP4_SSL(self.workflow.email_handler.config['imap_server'])
            imap_server.login(self.workflow.email_handler.config['email'], self.workflow.email_handler.config['password'])
            imap_server.select('INBOX')

            _, message_numbers = imap_server.search(None, 'ALL')
            for number in message_numbers[0].split():
                msg_number = int(number)
                _, msg_data = imap_server.fetch(number, '(RFC822)')
                raw_email = msg_data[0][1].decode('utf-8')
                email_message = message_from_string(raw_email)
                sender = email_message['From']
                original_subject = email_message['Subject'].strip()
                normalized_subject = self.normalize_subject(original_subject)
                business_name = EmailHandlerService.get_business_name_from_email(sender)

                if business_name in self.business_subject_criteria and re.search(self.business_subject_criteria[business_name], normalized_subject, re.IGNORECASE):
                    logging.getLogger('production').info(f"Added email from '{business_name}' with normalized subject '{normalized_subject}' to the queue.")
                    self._add_to_queue(msg_number, email_message)
                else:
                    self._denied_emails_count += 1
                    reason = "Subject does not match criteria or sender not recognized"
                    logging.getLogger('production').info(f"Denied email #{msg_number} from '{sender}' for reason: {reason}. Total denied: {self._denied_emails_count}")
                    self.workflow.email_handler.flag_email(original_subject)
        except Exception as e:
            logging.getLogger('debug').error(f"Error during mailbox scanning: {e}")
        finally:
            imap_server.logout()


        self.email_processor.process_next_email()

    def run(self):
        gui_thread = Thread(target=self.start_gui)
        gui_thread.daemon = True
        gui_thread.start()

        while True:
            current_time = datetime.datetime.now().time()
            
            #TEST LINE
            start_time = datetime.time(1, 0, 0)
            end_time = datetime.time(23, 0, 0)

            #PRODUCTION LINE
            #start_time = datetime.time(7, 0, 0)
            #end_time = datetime.time(19, 0, 0)

            if start_time <= current_time <= end_time:          
                self.scan_emails()
                logging.getLogger('production').info("Sleeping for next scan interval...")
                time.sleep(self.check_interval)
            else:
                now = datetime.datetime.now()
                next_start = datetime.datetime.combine(now.date(), start_time)
                if now.time() > end_time:
                    next_start += datetime.timedelta(days=1)
                
                sleep_seconds = (next_start - now).total_seconds()

                logging.getLogger('production').info(f"Outside active scanning hours. Sleeping for {sleep_seconds} seconds until next active window.")
                time.sleep(sleep_seconds)

    def normalize_subject(self, subject):
        return re.sub(r'[^a-zA-Z0-9]+', '', subject)

class EmailProcessorService:
    def __init__(self, workflow, email_queue, business_subject_criteria):
        self.workflow = workflow
        self.email_queue = email_queue
        self.business_subject_criteria = business_subject_criteria

    def add_to_queue(self, msg_number, email_message):
        timestamp = time.time()
        self.email_queue.put((msg_number, email_message, timestamp))

    def process_next_email(self):
        while not self.email_queue.empty():
            msg_number, email_message, timestamp = self.email_queue.get()
            sender = email_message['From']
            business_name = EmailHandlerService.get_business_name_from_email(sender)
            subject = email_message['Subject']
            logging.getLogger('production').info(f"Processing email from '{business_name}' with subject '{subject}'.")

            attachment = self.workflow.email_handler.extract_attachment_from_email(email_message)
            if attachment:
                self.workflow.run_workflow_with_attachment(attachment, sender, subject, msg_number)
            else:
                logging.getLogger('production').info("No attachment found in the email for processing.")

    def should_process_email(self, sender, subject):
        business_name = EmailHandlerService.get_business_name_from_email(sender)
        return business_name in self.business_subject_criteria and re.search(self.business_subject_criteria[business_name], subject)

class EmailHandlerService:
    def __init__(self, config_file='config.ini'):
        self.config = ConfigurationLoader.load_config(config_file)
        self.utilities = Utilities(config_file)

    @staticmethod
    def get_business_name_from_email(sender_email):
        match = re.search(r'[\w\.-]+@[\w\.-]+|\w+@[\w\.-]+|<([\w\.-]+@[\w\.-]+)>', sender_email)
        if match:
            extracted_email = match.group(1) if match.group(1) else match.group(0)
            email_parts = extracted_email.replace('<', '').replace('>', '').split("@")
            if len(email_parts) == 2:
                domain_parts = email_parts[1].split(".")
                if len(domain_parts) > 1:
                    business_name = domain_parts[0]
                    return business_name
        return None
    
    def extract_attachment_from_email(self, email_message):
        part_counter = 0
        for part in email_message.walk():
            part_counter += 1
            content_type = part.get_content_type()
            content_disposition = part.get('Content-Disposition', '').lower()
            filename = part.get_filename()
            
            #logging.getLogger('debug').info(f"Part {part_counter}: Type={content_type}, Disposition={content_disposition}, Filename={filename}")
            if "pdf" in content_type or (filename and filename.lower().endswith(".pdf")):
                if "attachment" in content_disposition or "inline" in content_disposition or not content_disposition:
                    return part.get_payload(decode=True)
            elif part.is_multipart():
                continue
      
        logging.getLogger('production').info("No PDF attachment found in the email.")
        return None

    def upload_to_ftp(self, updated_content, Creditor_number, Debtor_international_location_number, Invoice_number):
        ftp_hostname = self.config['ftp_hostname']
        ftp_location = self.config['ftp_location']
        ftp_username = self.config['ftp_username']
        ftp_password = self.config['ftp_password']

        filename = f"{Creditor_number}.{Debtor_international_location_number}.{Invoice_number}.xml"
        filename = Utilities.clean_special_characters(filename) 
        content_stream = io.BytesIO(updated_content.encode('utf-8'))

        try:
            with ftplib.FTP(ftp_hostname) as ftp:
                ftp.login(ftp_username, ftp_password)
                ftp.cwd(ftp_location)
                ftp.storbinary(f'STOR {filename}', content_stream)
            logging.getLogger('production').info("File uploaded successfully to FTP.")
            return True
        except Exception as e:
            logging.getLogger('production').error(f"Error uploading file to FTP: {e}")
            return False
    
    def upload_pdf_to_ftp(self, pdf_content, Creditor_number, Debtor_international_location_number, Invoice_number):
        ftp_hostname = self.config['ftp_hostname']
        ftp_location = self.config['ftp_location_pdf']
        ftp_username = self.config['ftp_username']
        ftp_password = self.config['ftp_password']

        filename = f"{Creditor_number}-{Debtor_international_location_number}.{Invoice_number}.pdf"
        filename = Utilities.clean_special_characters(filename) 
        content_stream = io.BytesIO(pdf_content)

        try:
            with ftplib.FTP(ftp_hostname) as ftp:
                ftp.login(ftp_username, ftp_password)
                ftp.cwd(ftp_location)
                ftp.storbinary(f'STOR {filename}', content_stream)
            logging.getLogger('production').info("PDF file uploaded successfully to FTP.")
            return True
        except Exception as e:
            logging.getLogger('production').error(f"Error uploading PDF file to FTP: {e}")
            return False

    def delete_email_by_subject(self, subject):
        try:
            with imaplib.IMAP4_SSL(self.config['imap_server']) as imap_server:
                imap_server.login(self.config['email'], self.config['password'])
                imap_server.select('INBOX')
                typ, data = imap_server.search(None, 'SUBJECT', f'"{subject}"')
                for num in data[0].split():
                    imap_server.store(num, '+FLAGS', '\\Deleted')
                imap_server.expunge()
        except Exception as e:
            logging.getLogger('production').error(f"Error deleting email by subject: {e}")

    def flag_email(self, subject):
        try:
            with imaplib.IMAP4_SSL(self.config['imap_server']) as imap_server:
                imap_server.login(self.config['email'], self.config['password'])
                imap_server.select('INBOX')
                typ, data = imap_server.search(None, f'(SUBJECT "{subject}")')
                if typ != 'OK':
                    logging.getLogger('debug').error(f"No emails found with subject: {subject}")
                    return
                for num in data[0].split():
                    imap_server.store(num, '+FLAGS', '\\Flagged')
                    logging.getLogger('debug').info(f"Email {subject} has been flagged as important.")
        except Exception as e:
            logging.getLogger('debug').error(f"Error flagging email {subject}: {e}")
# ------------------------
# Attachment and Document Analysis Section
# ------------------------
class AttachmentAnalyzer:
    debtor_map = ConfigurationLoader.load_debtor_map()

    def __init__(self, config_file='config.ini'):
        self.config = ConfigurationLoader.load_config(config_file)
        self.business_models_map = ConfigurationLoader.load_business_models_map()

    def analyze_attachment(self, attachment, business_name):
        business_info = self.business_models_map.get(business_name, {"model_id": "YOUR_CUSTOM_BUILT_MODEL", "Creditor_number": None, "Creditor_international_location_number": None})
        model_id = business_info["model_id"]

        try:
            document_analysis_client = DocumentAnalysisClient(
                endpoint=self.config['endpoint'],
                credential=AzureKeyCredential(self.config['key'])
            )
            poller = document_analysis_client.begin_analyze_document(model_id, attachment)
            result = poller.result()
            key_value_pairs = {name: (field.value if field.value else field.content) for document in result.documents for name, field in document.fields.items()}
            key_value_pairs["Creditor_number"] = business_info["Creditor_number"]
            key_value_pairs["Creditor_international_location_number"] = business_info["Creditor_international_location_number"]
            return key_value_pairs
        except Exception as e:
            logging.getLogger('debug').error(f"Error analyzing the attachment: {e}")
            return None

    @staticmethod
    def get_debtor_info(debtor_name):
        debtor_info = AttachmentAnalyzer.debtor_map.get(debtor_name, {})
        return debtor_info.get("illnr"), debtor_info.get("debtor_code")

    @staticmethod
    def document_field_to_dict(document_field):
        if isinstance(document_field, DocumentField):
            return document_field.to_dict()
        elif isinstance(document_field, list):
            return [AttachmentAnalyzer.document_field_to_dict(item) for item in document_field]
        else:
            return document_field

    @staticmethod
    def calculate_payment_dates(invoice_date_datetime: datetime, discount_days: int, non_discount_days: int) -> Tuple[datetime.datetime, datetime.datetime]:
        try:
            Payment_term_with_discount_date = invoice_date_datetime + datetime.timedelta(days=discount_days)
            Payment_term_without_discount_date = invoice_date_datetime + datetime.timedelta(days=non_discount_days)
            return Payment_term_with_discount_date, Payment_term_without_discount_date
        except Exception as e:
            logging.getLogger('debug').error(f"Error calculating payment dates: {e}")
            return None, None
# ------------------------
# Template and Utilities Management Section
# ------------------------
class TemplateManager:
    @staticmethod
    def replace_static_placeholders(template_content, replacements):
        try:
            for key, value in replacements.items():
                if isinstance(value, list):
                    value = ' '.join([str(item.content) if hasattr(item, 'content') else str(item) for item in value])
                elif value is None:
                    value = ''
                elif isinstance(value, datetime.datetime):
                    value = value.strftime('%d-%m-%Y')
                template_content = template_content.replace(f"[{key}]", str(value))
            return template_content
        except Exception as e:
            logging.getLogger('debug').error(f"Error in replace_static_placeholders: {e}")
            return None

    @staticmethod
    def extract_dynamic_segment_content(template_content, start_delimiter="<!-- Dynamic Segment Start -->", end_delimiter="<!-- Dynamic Segment End -->"):
        try:
            start_index = template_content.find(start_delimiter)
            end_index = template_content.find(end_delimiter)

            if start_index == -1 or end_index == -1:
                return None

            segment_content = template_content[start_index + len(start_delimiter):end_index].strip()
            return segment_content
        except Exception as e:
            logging.getLogger('debug').error(f"Error in extract_dynamic_segment_content: {e}")
            return None

    @staticmethod
    def replace_dynamic_placeholders(template_content, material_list):
        try:
            segment_content = TemplateManager.extract_dynamic_segment_content(template_content)
            if segment_content is None:
                logging.getLogger('debug').critical("Dynamic segment delimiters not found in the template.")
                return template_content

            populated_segments = []
            position_counter = 10
            for material in material_list:
                temp_segment = segment_content
                nested_material = material.get('value', {})

                # Assign 'Position_number' directly in 'nested_material' for consistency
                if 'Position_number' not in nested_material or nested_material['Position_number'] is None:
                    position_number = str(position_counter).zfill(4)
                    nested_material['Position_number'] = position_number  # Ensure 'Position_number' is in 'nested_material'
                    material['Position_number'] = position_number  # Optionally keep this line if 'Position_number' needs to be at the top level too
                    position_counter += 10

                placeholders = re.findall(r'\[([^\]]+)\]', temp_segment)
                for placeholder in placeholders:
                    replace_value = ""  # Default placeholder for missing keys
                    if placeholder in nested_material:
                        nested_value = nested_material[placeholder]
                        # Check if 'nested_value' is a dict and has 'value', or use it directly if it's string/int
                        if isinstance(nested_value, dict) and 'value' in nested_value:
                            replace_value = nested_value['value']
                        elif isinstance(nested_value, (str, int)):  # Directly use the value if it's string or int
                            replace_value = nested_value

                    temp_segment = temp_segment.replace(f'[{placeholder}]', str(replace_value))

                populated_segments.append(temp_segment)

            # Reconstruct the template with populated dynamic segments
            start_delimiter = "<!-- Dynamic Segment Start -->"
            end_delimiter = "<!-- Dynamic Segment End -->"
            template_start = template_content.split(start_delimiter)[0]
            template_end = template_content.split(end_delimiter)[-1]  # Use -1 to ensure we get the part after the last end delimiter
            updated_content = template_start + start_delimiter + '\n' + '\n'.join(populated_segments) + '\n' + end_delimiter + template_end
            
            return updated_content
        except Exception as e: 
            logging.getLogger('debug').error(f"Error in replace_dynamic_placeholders: {e}")
            return None

    @staticmethod
    def clean_template_content(content):
        try:
            content = re.sub(r'\[.*?\]', '', content)
            content = re.sub(r'(?<=<DATUM>)(\d{4})-(\d{2})-(\d{2})(?=</DATUM>)', r'\1\2\3', content)
            content = re.sub(r'(?<=<DATUM>)(\d{2})-(\d{2})-(\d{4})(?=</DATUM>)', r'\1\2\3', content)
            def currency_formatter(match):
                num_str = match.group()
                num_str = num_str.replace(' ', '')
                if ',' in num_str:
                    parts = num_str.split(',')
                    if '.' in parts[0]:
                        parts[0] = parts[0].replace('.', '')
                    return '.'.join(parts)
                return num_str
            currency_fields = ['BETRG', 'SUMME', 'MWSBT']
            for field in currency_fields:
                content = regex.sub(rf'(?<=<{field}>)[^<]+(?=</{field}>)', currency_formatter, content)
            return content
        except Exception as e:
            logging.getLogger('debug').error(f"Error in clean_template_content: {e}")
            return None

class Utilities:
    def __init__(self, config_file='config.ini'):
        self.config = ConfigurationLoader.load_config(config_file)

    def determine_tax_qualifier(self, partner_country_abbr, tax_percent):
        eu_country_abbreviations_map = ConfigurationLoader.load_EU_country_abbreviations_map()
        tax_qualifier_map = ConfigurationLoader.load_tax_qualifier_map()
        if partner_country_abbr == 'NL':
            country_category = "NL"
        elif partner_country_abbr in eu_country_abbreviations_map['EU_country_abbreviations']:
            country_category = "EU"
        else:
            country_category = "Non-EU"
        tax_qualifier_map = tax_qualifier_map.get(country_category)
        if tax_qualifier_map and str(tax_percent) in tax_qualifier_map:
            return tax_qualifier_map[str(tax_percent)]
        else:
            logging.getLogger('debug').critical(f"Tax code for {tax_percent}% in {country_category} not found.")
            return None

    def clean_special_characters(string):
        special_chars = ["/", "\\", ":", "*", "?", "\"", "<", ">", "|"]
        for char in special_chars:
            string = string.replace(char, "-")
        return string

    def replace_placeholders_with_values(temp_segment, material):
        # Find all placeholders in the form of [key]
        placeholders = re.findall(r'\[([^\]]+)\]', temp_segment)
        
        for placeholder in placeholders:
            key = placeholder  # The key is the string inside the brackets
            value = material.get(key, '')  # Get the corresponding value from the material dict; default to '' if key not found
            temp_segment = temp_segment.replace(f'[{key}]', str(value))  # Replace the placeholder with the value
            
        return temp_segment
# ------------------------
# Workflow Management Section
# ------------------------
class InvoiceWorkflowManager:
    def __init__(self, config_file='config.ini'):
        self.config = ConfigurationLoader.load_config(config_file)
        self.email_handler = EmailHandlerService(config_file)
        self.attachment_processor = AttachmentAnalyzer(config_file)
        self.utilities = Utilities(config_file)
        self.unit_map = ConfigurationLoader.load_unit_map()         

    def run_workflow_with_attachment(self, attachment, sender, subject, msg_number):
        business_name = EmailHandlerService.get_business_name_from_email(sender)
        if not business_name:
            logging.getLogger('debug').error("Invalid business name format in email")
            return

        key_value_pairs = self.attachment_processor.analyze_attachment(attachment, business_name)
        key_value_pairs = {k: AttachmentAnalyzer.document_field_to_dict(v) for k, v in key_value_pairs.items()}
        
        should_stop, type = self.check_invoice_type(key_value_pairs['Invoice_value'], subject)
        if should_stop:
            return

        if 'Invoice_date' not in key_value_pairs:
            logging.getLogger('debug').error("Invoice_date not found in the attachment.")
            return
        
        processed_key_value_pairs = self.generate_and_process_key_value_pairs(key_value_pairs, type)
        ftp_upload_success = self.prepare_and_upload_to_ftp(processed_key_value_pairs, attachment)
        if ftp_upload_success:
            print("succesfully uploaded to FTP")          
            #PRODUCTION LINE
            #self.email_handler.delete_email_by_subject(subject)
        else:
            logging.getLogger('production').error("FTP upload failed, email will be flagged instead of deleted.")
            #PRODUCTION LINE
            #self.email_handler.flag_email(subject)

    def check_invoice_type(self, invoice_value, subject):
        if invoice_value is None:
            self.email_handler.flag_email(subject)
            logging.getLogger('production').error("Invoice value cannot be extracted from pdf, email will be flagged instead of deleted.")
            return (True, None)
        if "-" in invoice_value:
            logging.getLogger('production').error("Negative invoice value detected, processing as credit note.")
            return (False, "CRME")
        try:
            normalized_invoice_value = invoice_value.replace(' ', '').replace('.', '').replace(',', '.')
            if float(normalized_invoice_value) == 0:
                #PRODUCTON LINE
                #self.email_handler.delete_email_by_subject(subject)
                logging.getLogger('production').info("The invoice is a null invoice and will be deleted")
                return (True, None)
        except (ValueError, TypeError) as e:
            self.email_handler.flag_email(subject)
            logging.getLogger('production').error(f"Error processing invoice value '{invoice_value}': {e}, email will be flagged.")
            return (True, None)  # Indicate that processing should stop because of an error
        return (False, "INVO")

    def generate_and_process_key_value_pairs(self, key_value_pairs, type):
        current_datetime = datetime.datetime.now()
        additional_data = {
            'Creation_date': current_datetime.strftime('%Y%m%d'),
            'Creation_time': current_datetime.strftime('%H%M%S'),
            'Timestamp': current_datetime.strftime('%Y%m%d%H%M%S')
        }

        invoice_date_str = key_value_pairs['Invoice_date']
        date_formats = ['%d-%m-%Y', '%d.%m.%Y', '%d.%m.%y', '%d%m%y']
        Invoice_date_datetime = None
        
        for date_format in date_formats:
            try:
                Invoice_date_datetime = datetime.datetime.strptime(invoice_date_str, date_format)
                break
            except ValueError:
                continue
        if Invoice_date_datetime is None:
            logging.getLogger('debug').error(f"Invalid date format: {invoice_date_str}")
            return
        
        key_value_pairs['Invoice_date'] = Invoice_date_datetime.strftime('%Y-%m-%d')

        purchase_order_date_str = key_value_pairs.get('Purchase_order_date', '')
        if not purchase_order_date_str:
            Purchase_order_date_datetime = None
        else:
            date_formats = ['%d-%m-%Y', '%d.%m.%Y', '%d.%m.%y']
            Purchase_order_date_datetime = None

            for date_format in date_formats:
                try:
                    Purchase_order_date_datetime = datetime.datetime.strptime(purchase_order_date_str, date_format)
                    break
                except ValueError:
                    continue

        if Purchase_order_date_datetime:
            key_value_pairs['Purchase_order_date'] = Purchase_order_date_datetime.strftime('%Y-%m-%d')
        else:
            logging.getLogger('debug').warning(f"Unable to parse Purchase_order_date: {purchase_order_date_str}")

        additional_data['Payment_term_with_discount_date'], additional_data['Payment_term_without_discount_date'] = \
            AttachmentAnalyzer.calculate_payment_dates(Invoice_date_datetime, int(key_value_pairs.get('Payment_term_with_discount_days') or 0), int(key_value_pairs.get('Payment_term_without_discount_days') or 0))

        debtor_illnr, debtor_code = AttachmentAnalyzer.get_debtor_info(key_value_pairs['Debtor_name'])
        key_value_pairs['Debtor_code'] = []
        if debtor_illnr:
            key_value_pairs['Debtor_international_location_number'] = debtor_illnr
        if debtor_code:
            key_value_pairs['Debtor_code'] = debtor_code
        
        country_abbreviations_map = ConfigurationLoader.load_country_abbreviations_map()          
        if 'Partner_country' in key_value_pairs:
            country_name = key_value_pairs['Partner_country']
            country_abbr = country_abbreviations_map.get(country_name)
            if country_abbr:
                key_value_pairs['Partner_country'] = country_abbr

        tax_percent_raw = key_value_pairs.get('Tax_percent', '')
        if tax_percent_raw is not None:
            tax_percent_clean = re.sub(r"[^\d.,]", "", tax_percent_raw)
            tax_percent_clean = tax_percent_clean.replace('.', '').replace(',', '').rstrip('0').rstrip('.')
        if tax_percent_raw is None:
            tax_percent_clean = tax_percent_raw
        if not tax_percent_clean:
            tax_percent_clean = '0'
        key_value_pairs['Tax_percent'] = tax_percent_clean
        partner_country_abbr = key_value_pairs['Partner_country'] 
        tax_qualifier = self.utilities.determine_tax_qualifier(partner_country_abbr, tax_percent_clean)
        if tax_qualifier:
            key_value_pairs['Tax_qualifier'] = tax_qualifier
        else:
            logging.getLogger('debug').error(f"Tax code for {tax_percent_clean} could not be determined")

        if 'Unit' in key_value_pairs:
            unit_value = key_value_pairs['Unit']
            mapped_unit_value = self.unit_map.get(unit_value)
            if mapped_unit_value:
                key_value_pairs['Unit'] = mapped_unit_value

        if 'Debtor_number' in key_value_pairs:
            debtor_number_string = key_value_pairs['Debtor_number']
            cleaned_debtor_number_string = Utilities.clean_special_characters(debtor_number_string)
            if cleaned_debtor_number_string:
                key_value_pairs['Debtor_number'] = cleaned_debtor_number_string        
        
        invoice_number = Utilities.clean_special_characters(key_value_pairs.get('Invoice_number'))
        key_value_pairs['Invoice_number'] = invoice_number

        try:
            purchase_order_line = key_value_pairs['Material_list'][0]['value']['Purchase_order_line']['value']
            if purchase_order_line is None and key_value_pairs['Purchase_order'] is not None:
                purchase_order_line = key_value_pairs['Purchase_order']
                for item in key_value_pairs['Material_list']:
                    if 'value' in item and 'Purchase_order_line' in item['value']:
                        item['value']['Purchase_order_line']['value'] = purchase_order_line
            if purchase_order_line is not None and key_value_pairs['Purchase_order'] is None:
                key_value_pairs['Purchase_order'] = purchase_order_line
                print(key_value_pairs['Purchase_order'])
        except KeyError:
            purchase_order = key_value_pairs['Purchase_order']
            purchase_order_line = purchase_order
            key_value_pairs['Purchase_order_line'] = purchase_order_line
            for item in key_value_pairs.get('Material_list', []):
                if 'value' in item and 'Purchase_order_line' not in item['value']:
                    item['value']['Purchase_order_line'] = {'value': purchase_order_line}
        
        key_value_pairs['Type'] = type

        if type == "CRME":
            key_value_pairs['Invoice_value'] = key_value_pairs['Invoice_value'].replace('-', '')
            key_value_pairs['Net_value'] = key_value_pairs['Net_value'].replace('-', '')
            key_value_pairs['Total_tax'] = key_value_pairs['Total_tax'].replace('-', '')
            key_value_pairs['Material_list'][0]['value']['Quantity']['value'] = key_value_pairs['Material_list'][0]['value']['Quantity']['value'].replace('-', '')

        return key_value_pairs

    def prepare_and_upload_to_ftp(self, key_value_pairs, original_pdf):
        config_file='config.ini'
        config = ConfigurationLoader.load_config(config_file)
        try:
            with open(config["path_six"], 'r', encoding='utf-8') as file:
                template_content = file.read()
            statically_updated_content = TemplateManager.replace_static_placeholders(template_content, key_value_pairs)
            dynamically_updated_content = TemplateManager.replace_dynamic_placeholders(statically_updated_content, key_value_pairs.get('Material_list', []))
            cleaned_content = TemplateManager.clean_template_content(dynamically_updated_content)


            Creditor_number = key_value_pairs.get('Creditor_number')
            Debtor_international_location_number = key_value_pairs.get('Debtor_international_location_number')
            Invoice_number = key_value_pairs.get('Invoice_number')

            # TEST LINE: Save populated XML to local IDOCs directory for test environment
            TestEnvironmentUtilities.save_xml_locally_for_test(key_value_pairs, cleaned_content)

            xml_success = self.email_handler.upload_to_ftp(cleaned_content, Creditor_number, Debtor_international_location_number, Invoice_number)
            pdf_success = True
            if original_pdf:
                pdf_success = self.email_handler.upload_pdf_to_ftp(original_pdf, Creditor_number, Debtor_international_location_number, Invoice_number)
            else:
                logging.getLogger('debug').warning("Original PDF is not available for upload.")
            return xml_success and pdf_success
        except Exception as e:
            logging.getLogger('debug').error(f"Error in preparing or uploading to FTP: {e}")
# -------------------
# Main Application
# -------------------
def main():
    LoggingManager.setup_logging()
    email_scanner_service = EmailScannerService()
    email_scanner_service.run()

if __name__ == "__main__":
    main()