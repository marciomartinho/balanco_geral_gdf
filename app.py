from flask import Flask, render_template, jsonify, request
from servicos.caixa_servico import CaixaServico

app = Flask(__name__)

@app.route('/')
def index():
    # A rota principal agora renderiza a nova tela de resumo
    return render_template('resumo_caixa_ug.html')

@app.route('/api/filtros/anos')
def api_anos():
    servico = CaixaServico()
    anos = servico.buscar_anos_disponiveis()
    return jsonify(anos)

@app.route('/api/filtros/ugs')
def api_ugs():
    ano = request.args.get('ano', type=int)
    if not ano:
        return jsonify({'erro': 'O ano é obrigatório'}), 400
    servico = CaixaServico()
    ugs = servico.buscar_ugs_por_ano(ano)
    return jsonify(ugs)

@app.route('/api/caixa/resumo_ug')
def api_resumo_ug():
    ano = request.args.get('ano', type=int)
    ug = request.args.get('ug', type=int)
    
    if not ano or not ug:
        return jsonify({'erro': 'Ano e UG são obrigatórios'}), 400
        
    servico = CaixaServico()
    dados = servico.buscar_resumo_por_ug(ano=ano, ug=ug)
    return jsonify(dados)

if __name__ == '__main__':
    app.run(debug=True, port=5000)