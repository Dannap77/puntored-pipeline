# Resumen Ejecutivo — Análisis de Transacciones

**Periodo analizado:** enero 2024 – marzo 2025
**Volumen:** 4,970 transacciones · 500 usuarios · $6,655 M procesados
**Tasa de éxito global:** 92.3% · **Tiempo de procesamiento promedio:** 602 ms

---

## Insight 1 — Fricción puntual: Wallet + Mobile

La combinación **wallet en canal mobile** concentra la peor tasa de fallo del catálogo:
**18.3% de rechazo** (vs. 7.7% de fallo global), sobre 426 transacciones.

| Combinación | Tasa fallo | Volumen |
|---|---|---|
| **wallet · mobile** | **18.3%** | 426 |
| cash · web | 16.6% | 163 |
| wallet · web | 17.2% | 227 |

> 🎯 **Recomendación:** Priorizar una auditoría de la integración wallet en la app móvil
> antes de la próxima campaña. Revisar logs de error específicos, tasas de timeout y
> compatibilidad de versiones del SDK. Una mejora de 5 pp en esta combinación
> recuperaría ~21 transacciones/mes.

---

## Insight 2 — El canal Web es 3.1× más lento que API

| Canal | Tiempo promedio | Volumen |
|---|---|---|
| API | 250 ms | 805 |
| Mobile | 598 ms | 2,814 |
| **Web** | **786 ms** | **1,585** |

La latencia en web no se traduce en menor success rate (87.1% vs 87.1% API), pero **3.1× más
tiempo en pantalla** correlaciona típicamente con abandono pre-confirmación.

> 🎯 **Recomendación:** Identificar si el cuello de botella está en validaciones del lado
> cliente, render de la pasarela o llamadas síncronas evitables. El canal web representa
> 32% del volumen — bajar la latencia a niveles mobile (≈600 ms) impacta directamente la
> conversión sin requerir cambios en backend de pagos.

---

## Insight 3 — Concentración en Card: 45% del volumen económico

**Card** procesa $3,025 M (45% del total), con tasa de éxito 88.2% — la más alta del catálogo.
Pero también es **el método con mayor riesgo de concentración**: si la pasarela de tarjetas
sufre una caída, casi la mitad del negocio se afecta.

| Método | Volumen ($) | % del total | Tasa éxito |
|---|---|---|---|
| Card | $3,025 M | 45% | 88.2% |
| PSE | $1,288 M | 19% | 86.6% |
| Wallet | $968 M | 15% | 84.2% |
| Cash | $694 M | 10% | 86.4% |
| Bank transfer | $680 M | 10% | 86.7% |

> 🎯 **Recomendación:** Implementar **fallback automático entre pasarelas de card** (si la
> primaria falla, reintentar con secundaria). Adicionalmente, lanzar incentivos puntuales
> para PSE y wallet (cashback) que diversifiquen el mix sin sacrificar success rate.

---

## Síntesis para Estrategia

1. **Producto/Mobile** tiene un quick-win en wallet+mobile (insight 1).
2. **Frontend web** debe priorizar performance (insight 2).
3. **Riesgo y producto** deben construir resiliencia frente a una caída de card (insight 3).
