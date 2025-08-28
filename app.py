from flask import Flask, render_template, jsonify
from servicos.caixa_servico import CaixaServico

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('tabela_caixa.html')

@app.route('/api/caixa/detalhes')
def api_caixa_detalhes():
    servico = CaixaServico()
    dados = servico.buscar_detalhes_caixa(limite=100)
    return jsonify(dados)

if __name__ == '__main__':
    app.run(debug=True, port=5000)