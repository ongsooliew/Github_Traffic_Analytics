mkdir Results
mkdir 'Json Files'

conda init
conda create -y --name sooliewenv python=3.8
activate sooliewenv
pip install -r requirements.txt

