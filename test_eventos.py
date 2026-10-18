import unittest

# Simulamos la lógica central de evaluación que tiene tu consumidor
def evaluar_alerta(event_type):
    if event_type == 'sudden braking':
        return True
    return False

class TestV2XLogic(unittest.TestCase):
    
    def test_detectar_frenada_brusca(self):
        # Comprueba que un evento de frenada dispara la alerta (True)
        resultado = evaluar_alerta('sudden braking')
        self.assertTrue(resultado, "El sistema DEBE detectar una frenada brusca como alerta")

    def test_ignorar_eventos_rutinarios(self):
        # Comprueba que la telemetría normal no dispara falsas alarmas (False)
        resultado_pos = evaluar_alerta('position')
        resultado_haz = evaluar_alerta('hazard')
        self.assertFalse(resultado_pos, "Un evento de posición NO debe generar alerta")
        self.assertFalse(resultado_haz, "Un aviso de hazard normal NO debe generar alerta crítica")

if __name__ == '__main__':
    unittest.main()