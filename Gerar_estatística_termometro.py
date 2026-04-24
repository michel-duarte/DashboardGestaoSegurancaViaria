import os
import json
import re

def extrair_dados_estatisticos(diretorio_entrada):
    resultados = []

    # Regex para capturar o conteúdo após "var dadosDia ="
    regex_json = re.compile(r"var\s+dadosDia\s*=\s*({.*?});", re.DOTALL)

    if not os.path.exists(diretorio_entrada):
        print(f"Erro: A pasta '{diretorio_entrada}' não foi encontrada.")
        return

    for nome_arquivo in os.listdir(diretorio_entrada):
        if nome_arquivo.endswith(".js"):
            caminho_completo = os.path.join(diretorio_entrada, nome_arquivo)
            
            try:
                with open(caminho_completo, 'r', encoding='utf-8') as f:
                    conteudo = f.read()
                    match = regex_json.search(conteudo)
                    
                    if match:
                        json_data = json.loads(match.group(1))
                        diff_global = json_data.get("metadata", {}).get("diff_global", 0)
                        
                        resultados.append({
                            "arquivo": nome_arquivo,
                            "diff_global": diff_global
                        })
            except Exception as e:
                print(f"Erro ao processar {nome_arquivo}: {e}")

    if not resultados:
        print("Nenhum dado encontrado.")
        return

    # Ordenar por valor de diff_global
    resultados_ordenados = sorted(resultados, key=lambda x: x['diff_global'], reverse=True)

    # Pegar as 3 maiores (positivas) e 3 menores (negativas)
    maiores_3 = [r for r in resultados_ordenados if r['diff_global'] > 0][:3]
    menores_3 = [r for r in resultados_ordenados if r['diff_global'] < 0][-3:]

    # Estrutura final do arquivo de saída
    estatisticas = {
        "maiores_diferencas_positivas": maiores_3,
        "menores_diferencas_negativas": menores_3
    }

    # Gerar o arquivo estatistica_termômetro.js
    with open('estatistica_termômetro.js', 'w', encoding='utf-8') as f_out:
        f_out.write("var estatisticasTermometro = ")
        json.dump(estatisticas, f_out, indent=4, ensure_ascii=False)
        f_out.write(";")

    print("Arquivo 'estatistica_termômetro.js' gerado com sucesso!")


# Execução
if __name__ == "__main__":
    # Certifique-se de que a pasta 'Dados_diarios' está no mesmo diretório do script
    extrair_dados_estatisticos("dados_diarios")