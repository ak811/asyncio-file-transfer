import logging

# The robustness tests deliberately send malformed traffic; keep expected server warnings quiet.
logging.getLogger("filetransfer").setLevel(logging.CRITICAL)
