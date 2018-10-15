import os

MONGO_DB = "suzi_scores"
MONGO_COLLECTION = "suzi_scores"
MONGO_HOST = "sixteencorea"
ES_HOST = "elasticsearch08"
ES_INDEX = "equity_analysis"


MODEL_PATH = os.environ.get("SUZI_MODEL_PATH", "/home/martin/projects/sigmai-suzi/chkpts/smaller-best.hdf5")
VOCAB_PATH = os.environ.get("SUZI_VOCAB_PATH", "/data/suzi/v1/id2w.pkl")
