import argparse

class NumberSet100:
    """
    Conjunto de los primeros 100 números naturales (1..100).
    - extract(n): “extrae” el número n (validación incluida).
    - find_missing(): calcula el faltante usando XOR (O(n) tiempo, O(1) espacio).


    El valor 100 (base 2) es igual a calcular (1 XOR 2 XOR 3 XOR ... 100), es decir, 1^2^3...^100=100
    
        Así, evitamos calcularlo cada vez, que sería procesar cada vez:
            data_structure = 0
            for i in range(1, 100+1):
                data_structure ^= i
    """
    def __init__(self):
        self.N = 100
        self.extracted = None

    def _validate(self, n):
        if not isinstance(n, int):
            raise ValueError("El valor debe ser un entero.")
        if n < 1 or n > self.N:
            raise ValueError("El número debe estar entre 1 y 100.")

    def extract(self, n: int):
        self._validate(n)
        self.extracted = n

    def find_missing(self) -> int:
        if self.extracted is None:
            raise RuntimeError("Primero debes extraer un número con extract(n).")

        N_minus_extracted = self.N ^ self.extracted
        faltante = self.N ^ N_minus_extracted
        return faltante

def run_cli():
    parser = argparse.ArgumentParser(
        description="Calcula el número faltante al extraer un valor del conjunto 1..100."
    )
    parser.add_argument(
        "--extract", type=int, required=True,
        help="Número a extraer (1..100)."
    )
    args = parser.parse_args()

    s = NumberSet100()
    try:
        s.extract(args.extract)
        missing = s.find_missing()

        print(f"Extraído: {args.extract}")
        print(f"Faltante calculado: {missing}")

        all_xor = s.N
        extraido = s.extracted
        now = all_xor ^ extraido
        result = all_xor ^ now
        print(f"""
            Proceso:
            ALL= 1^2^...^100 =     {bin(all_xor)}
            NOW = ALL^EXTRAIDO =   {bin(all_xor)} ^ {bin(extraido)} = {bin(now)}
            RESULT = ALL^NOW =     {bin(all_xor)} ^ {bin(now)} = {bin(missing)} = {missing}
        """)


    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    run_cli()