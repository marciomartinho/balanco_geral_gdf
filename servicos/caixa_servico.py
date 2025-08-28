"""
Serviço de Caixa e Equivalente de Caixa
USA queries SQL dos arquivos em dados/consultas/
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
from dados.conexao import get_db_manager
from dados.gerenciador_sql import get_gerenciador_sql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CaixaServico:
   """
   Serviço para gerenciar dados de Caixa e Equivalente de Caixa
   NÃO tem SQL hardcoded - lê de arquivos!
   """
   
   def __init__(self):
       self.db = get_db_manager()
       self.sql = get_gerenciador_sql()
       logger.info("Serviço de Caixa inicializado")
   
   def buscar_saldos_resumido(
       self, 
       ano: Optional[int] = None,
       mes: Optional[int] = None,
       ug: Optional[str] = None
   ) -> Dict[str, Any]:
       """
       Busca saldos resumidos usando query do arquivo SQL
       
       Args:
           ano: Ano de referência (padrão: ano atual)
           mes: Mês limite (padrão: mês atual)
           ug: Código da UG específica (padrão: todas)
       
       Returns:
           Dicionário com resumo e comparativo de saldos
       """
       ano = ano or datetime.now().year
       mes = mes or datetime.now().month
       ano_anterior = ano - 1
       
       logger.info(f"📊 Buscando saldos: {ano}/{ano_anterior} até mês {mes}")
       
       # Carregar query do arquivo caixa_equivalente.sql
       query, params = self.sql.preparar_query(
           'caixa_equivalente',  # Nome do arquivo (sem .sql)
           filtro_ug=ug,
           ano_atual=ano,
           ano_anterior=ano_anterior,
           mes_limite=mes
       )
       
       try:
           with self.db.get_cursor() as cursor:
               cursor.execute(query, params)
               colunas = [col[0] for col in cursor.description]
               resultados = cursor.fetchall()
           
           # Converter para lista de dicionários
           dados = [dict(zip(colunas, row)) for row in resultados]
           
           # Processar dados (lógica de negócio fica no serviço)
           resumo = self._calcular_resumo(dados, ano, ano_anterior)
           
           logger.info(f"✅ {len(dados)} registros encontrados")
           
           return {
               'sucesso': True,
               'periodo': {
                   'ano_atual': ano,
                   'ano_anterior': ano_anterior,
                   'mes': mes,
                   'ug': ug or 'CONSOLIDADO'
               },
               'resumo': resumo,
               'total_registros': len(dados)
           }
           
       except Exception as e:
           logger.error(f"❌ Erro ao buscar saldos: {e}")
           return {
               'sucesso': False,
               'erro': str(e),
               'resumo': None
           }
   
   def buscar_detalhes_caixa(
       self,
       ano: Optional[int] = None,
       mes: Optional[int] = None,
       limite: int = 100
   ) -> Dict[str, Any]:
       """
       Busca detalhes de caixa usando SQL do arquivo
       Calcula o saldo (débito - crédito) para cada linha
       
       Args:
           ano: Ano de referência
           mes: Mês limite
           limite: Número máximo de registros
       
       Returns:
           Dicionário com dados detalhados incluindo saldo calculado
       """
       ano = ano or datetime.now().year
       mes = mes or datetime.now().month
       
       logger.info(f"📋 Buscando detalhes: {ano} até mês {mes} (limite: {limite})")
       
       # Carregar query do arquivo
       query = self.sql.carregar_query('caixa_equivalente')
       
       # Adicionar filtros e limite
       query = f"""
           SELECT * FROM (
               {query}
               AND COEXERCICIO = :ano
               ORDER BY INMES DESC, COCONTACONTABIL, COUG
           ) WHERE ROWNUM <= :limite
       """
       
       try:
           with self.db.get_cursor() as cursor:
               cursor.execute(query, {
                   'ano': ano,
                   'ano_atual': ano,
                   'ano_anterior': ano - 1,
                   'mes_limite': mes,
                   'limite': limite
               })
               colunas = [col[0] for col in cursor.description]
               resultados = cursor.fetchall()
           
           # Processar dados e CALCULAR SALDO
           dados_processados = []
           saldo_total = 0
           
           for row in resultados:
               registro = dict(zip(colunas, row))
               
               # CÁLCULO DO SALDO (lógica de negócio no serviço!)
               vadebito = float(registro.get('VADEBITO', 0) or 0)
               vacredito = float(registro.get('VACREDITO', 0) or 0)
               saldo = vadebito - vacredito
               
               registro['VADEBITO'] = vadebito
               registro['VACREDITO'] = vacredito
               registro['SALDO'] = saldo
               
               saldo_total += saldo
               dados_processados.append(registro)
           
           logger.info(f"✅ {len(dados_processados)} registros processados")
           logger.info(f"💰 Saldo total: R$ {saldo_total:,.2f}")
           
           return {
               'sucesso': True,
               'dados': dados_processados,
               'total': len(dados_processados),
               'saldo_total': saldo_total
           }
           
       except Exception as e:
           logger.error(f"❌ Erro ao buscar detalhes: {e}")
           return {
               'sucesso': False,
               'erro': str(e),
               'dados': []
           }
   
   def buscar_evolucao_mensal(
       self,
       ano: Optional[int] = None
   ) -> Dict[str, Any]:
       """
       Busca evolução mensal do saldo de caixa
       
       Args:
           ano: Ano de referência
       
       Returns:
           Dados formatados para gráfico de evolução mensal
       """
       ano = ano or datetime.now().year
       
       logger.info(f"📈 Buscando evolução mensal: {ano}")
       
       # Podemos criar um arquivo SQL específico ou usar o existente
       query = """
           SELECT
               INMES as MES,
               SUM(VADEBITO - VACREDITO) as SALDO
           FROM
               MIL2001.SALDOCONTABIL_EX
           WHERE
               COEXERCICIO = :ano
               AND COCONTACONTABIL BETWEEN 111000000 AND 111999999
           GROUP BY INMES
           ORDER BY INMES
       """
       
       try:
           with self.db.get_cursor() as cursor:
               cursor.execute(query, {'ano': ano})
               resultados = cursor.fetchall()
           
           # Preparar dados para gráfico
           meses_nomes = [
               'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
               'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'
           ]
           
           evolucao = {
               'meses': [],
               'valores': [],
               'valores_acumulados': []
           }
           
           saldo_acumulado = 0
           for mes, saldo in resultados:
               saldo = float(saldo or 0)
               saldo_acumulado += saldo
               
               evolucao['meses'].append(meses_nomes[mes - 1])
               evolucao['valores'].append(saldo)
               evolucao['valores_acumulados'].append(saldo_acumulado)
           
           return {
               'sucesso': True,
               'ano': ano,
               'evolucao': evolucao
           }
           
       except Exception as e:
           logger.error(f"❌ Erro ao buscar evolução: {e}")
           return {
               'sucesso': False,
               'erro': str(e)
           }
   
   def buscar_top_ugs(
       self,
       ano: Optional[int] = None,
       mes: Optional[int] = None,
       limite: int = 10
   ) -> Dict[str, Any]:
       """
       Busca as UGs com maior saldo de caixa
       
       Args:
           ano: Ano de referência
           mes: Mês limite
           limite: Número de UGs a retornar
       
       Returns:
           Ranking das UGs por saldo
       """
       ano = ano or datetime.now().year
       mes = mes or datetime.now().month
       
       query = """
           SELECT * FROM (
               SELECT
                   COUG as UG,
                   MAX(NOUG) as NOME_UG,
                   SUM(VADEBITO - VACREDITO) as SALDO_TOTAL
               FROM
                   MIL2001.SALDOCONTABIL_EX
               WHERE
                   COEXERCICIO = :ano
                   AND COCONTACONTABIL BETWEEN 111000000 AND 111999999
                   AND INMES <= :mes
               GROUP BY COUG
               ORDER BY SALDO_TOTAL DESC
           ) WHERE ROWNUM <= :limite
       """
       
       try:
           with self.db.get_cursor() as cursor:
               cursor.execute(query, {'ano': ano, 'mes': mes, 'limite': limite})
               colunas = [col[0] for col in cursor.description]
               resultados = cursor.fetchall()
           
           ranking = []
           for i, row in enumerate(resultados, 1):
               dados = dict(zip(colunas, row))
               dados['POSICAO'] = i
               dados['SALDO_TOTAL'] = float(dados.get('SALDO_TOTAL', 0))
               ranking.append(dados)
           
           return {
               'sucesso': True,
               'ranking': ranking,
               'total': len(ranking)
           }
           
       except Exception as e:
           logger.error(f"❌ Erro ao buscar ranking: {e}")
           return {'sucesso': False, 'erro': str(e)}
   
   def _calcular_resumo(
       self, 
       dados: List[Dict], 
       ano_atual: int, 
       ano_anterior: int
   ) -> Dict[str, Any]:
       """
       Calcula resumo estatístico dos dados
       LÓGICA DE NEGÓCIO - fica no serviço, não no SQL!
       
       Args:
           dados: Lista de registros do banco
           ano_atual: Ano atual
           ano_anterior: Ano anterior
       
       Returns:
           Resumo com totais, variações e estatísticas
       """
       # Primeiro, calcular saldo para cada registro
       for registro in dados:
           vadebito = float(registro.get('VADEBITO', 0) or 0)
           vacredito = float(registro.get('VACREDITO', 0) or 0)
           registro['SALDO'] = vadebito - vacredito
       
       # Separar dados por ano
       dados_atual = [d for d in dados if d.get('COEXERCICIO') == ano_atual]
       dados_anterior = [d for d in dados if d.get('COEXERCICIO') == ano_anterior]
       
       # Calcular saldos totais
       saldo_atual = sum(d['SALDO'] for d in dados_atual)
       saldo_anterior = sum(d['SALDO'] for d in dados_anterior)
       
       # Calcular variação percentual
       variacao = 0
       if saldo_anterior != 0:
           variacao = ((saldo_atual / saldo_anterior) - 1) * 100
       
       # Estatísticas adicionais
       ugs_unicas = len(set(d.get('COUG', '') for d in dados if d.get('COUG')))
       contas_unicas = len(set(d.get('COCONTACONTABIL', '') for d in dados if d.get('COCONTACONTABIL')))
       
       return {
           'saldo_atual': saldo_atual,
           'saldo_anterior': saldo_anterior,
           'variacao_percentual': round(variacao, 2),
           'variacao_absoluta': saldo_atual - saldo_anterior,
           'total_ugs': ugs_unicas,
           'total_contas': contas_unicas,
           'registros_ano_atual': len(dados_atual),
           'registros_ano_anterior': len(dados_anterior)
       }
   
   def exportar_para_excel(
       self, 
       dados: Dict, 
       arquivo: str = 'caixa_equivalente.xlsx'
   ) -> str:
       """
       Exporta dados para Excel
       
       Args:
           dados: Dicionário com os dados
           arquivo: Nome do arquivo de saída
       
       Returns:
           Caminho do arquivo gerado
       """
       import pandas as pd
       
       # Verificar se há dados para exportar
       if not dados.get('sucesso') or not dados.get('dados'):
           raise ValueError("Sem dados para exportar")
       
       # Criar DataFrame
       df = pd.DataFrame(dados['dados'])
       
       # Criar diretório se não existir
       Path('arquivos/exportacoes').mkdir(parents=True, exist_ok=True)
       
       # Salvar arquivo
       caminho = f"arquivos/exportacoes/{arquivo}"
       df.to_excel(caminho, index=False)
       
       logger.info(f"📁 Dados exportados: {caminho}")
       return caminho


def testar_servico():
   """Testa as principais funcionalidades do serviço"""
   servico = CaixaServico()
   
   print("\n" + "="*60)
   print("TESTE DO SERVIÇO DE CAIXA E EQUIVALENTE")
   print("="*60)
   
   # Teste 1: Resumo
   print("\n📊 Testando buscar_saldos_resumido...")
   resultado = servico.buscar_saldos_resumido()
   
   if resultado['sucesso']:
       resumo = resultado['resumo']
       print(f"✅ Saldo Atual: R$ {resumo['saldo_atual']:,.2f}")
       print(f"✅ Saldo Anterior: R$ {resumo['saldo_anterior']:,.2f}")
       print(f"✅ Variação: {resumo['variacao_percentual']:.2f}%")
       print(f"✅ Total de UGs: {resumo['total_ugs']}")
   else:
       print(f"❌ Erro: {resultado['erro']}")
   
   # Teste 2: Detalhes
   print("\n📋 Testando buscar_detalhes_caixa...")
   detalhes = servico.buscar_detalhes_caixa(limite=5)
   
   if detalhes['sucesso']:
       print(f"✅ {detalhes['total']} registros encontrados")
       print(f"✅ Saldo total: R$ {detalhes.get('saldo_total', 0):,.2f}")
       
       # Mostrar primeiros registros
       if detalhes['dados']:
           print("\nPrimeiros registros:")
           for i, reg in enumerate(detalhes['dados'][:3], 1):
               print(f"  {i}. UG: {reg.get('COUG')} - Saldo: R$ {reg.get('SALDO', 0):,.2f}")
   else:
       print(f"❌ Erro: {detalhes['erro']}")
   
   print("\n" + "="*60)


if __name__ == "__main__":
   testar_servico()