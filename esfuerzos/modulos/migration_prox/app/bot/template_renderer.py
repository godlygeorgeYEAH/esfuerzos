"""
Motor de renderizado de templates para mensajes del bot.
Reemplaza variables dinámicas en los templates con datos reales.
"""
import re
from typing import Dict, Any, Optional


def render_template(template: str, variables: Dict[str, Any]) -> str:
    """
    Renderiza un template reemplazando {variable} con sus valores.
    Soporta: {nombre}, {nombre|default}, {nested.prop}
    """
    if not template:
        return ""
    if not variables:
        variables = {}

    pattern = r'\{([^{}|]+)(\|([^{}]*))?\}'

    def replace_variable(match):
        var_name = match.group(1).strip()
        default_value = match.group(3) if match.group(3) is not None else ""
        value = get_nested_value(variables, var_name)
        if value is None:
            return default_value
        return str(value)

    return re.sub(pattern, replace_variable, template)


def get_nested_value(data: Dict[str, Any], key_path: str) -> Optional[Any]:
    """Obtiene un valor usando notación de punto (ej: 'negocio.nombre')."""
    if not data or not key_path:
        return None
    if '.' not in key_path:
        return data.get(key_path)
    keys = key_path.split('.')
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
            if current is None:
                return None
        else:
            return None
    return current


def render_articulo_list(articulos: list, template: Optional[str] = None) -> str:
    """
    Renderiza una lista de Articulos en formato de texto para WhatsApp.

    Usa los campos del modelo Articulo: nombre, precio, descripcion.
    """
    if not articulos:
        return "No hay artículos disponibles en este momento."

    if template is None:
        template = "{index}. {nombre} - ${precio}"

    lines = []
    for idx, articulo in enumerate(articulos, start=1):
        if hasattr(articulo, '__dict__'):
            articulo_dict = {
                'nombre': getattr(articulo, 'nombre', ''),
                'precio': getattr(articulo, 'precio', 0),
                'descripcion': getattr(articulo, 'descripcion', None),
            }
        else:
            articulo_dict = articulo
        articulo_dict['index'] = idx
        lines.append(render_template(template, articulo_dict))

    return '\n'.join(lines)



def render_working_hours(start_time, end_time, working_days: list, template: Optional[str] = None) -> str:
    """Renderiza información de horarios de trabajo."""
    day_translation = {
        'monday': 'Lunes', 'tuesday': 'Martes', 'wednesday': 'Miércoles',
        'thursday': 'Jueves', 'friday': 'Viernes', 'saturday': 'Sábado', 'sunday': 'Domingo'
    }

    def format_time(time_obj):
        if not time_obj:
            return ""
        try:
            if hasattr(time_obj, 'hour'):
                hour, minute = time_obj.hour, time_obj.minute
            elif isinstance(time_obj, str):
                hour, minute = map(int, time_obj.split(':'))
            else:
                return str(time_obj)
            period = "AM" if hour < 12 else "PM"
            hour_12 = hour if hour <= 12 else hour - 12
            hour_12 = 12 if hour_12 == 0 else hour_12
            return f"{hour_12}:{minute:02d} {period}"
        except Exception:
            return str(time_obj)

    start_formatted = format_time(start_time)
    end_formatted = format_time(end_time)

    if not working_days:
        days_text = "todos los días"
    elif len(working_days) == 7:
        days_text = "todos los días"
    elif len(working_days) == 5 and all(d in working_days for d in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']):
        days_text = "Lunes a Viernes"
    elif len(working_days) == 6 and 'sunday' not in working_days:
        days_text = "Lunes a Sábado"
    else:
        days_es = [day_translation.get(d, d) for d in working_days]
        if len(days_es) == 1:
            days_text = days_es[0]
        elif len(days_es) == 2:
            days_text = f"{days_es[0]} y {days_es[1]}"
        else:
            days_text = ", ".join(days_es[:-1]) + f" y {days_es[-1]}"

    if template is None:
        return f"{days_text} de {start_formatted} a {end_formatted}"

    return render_template(template, {'days': days_text, 'start_time': start_formatted, 'end_time': end_formatted})


def render_payment_methods(payment_methods: list) -> str:
    """Renderiza lista de métodos de pago."""
    if not payment_methods:
        return "Efectivo"
    method_translation = {
        'cash': 'Efectivo', 'zelle': 'Zelle', 'paypal': 'PayPal',
        'bank_transfer': 'Transferencia bancaria', 'crypto': 'Criptomonedas',
        'binance': 'Binance Pay', 'mobile_payment': 'Pago móvil',
        'efectivo': 'Efectivo', 'transferencia': 'Transferencia bancaria',
    }
    methods_es = [method_translation.get(m, m) for m in payment_methods]
    if len(methods_es) == 1:
        return methods_es[0]
    elif len(methods_es) == 2:
        return f"{methods_es[0]} y {methods_es[1]}"
    else:
        return ", ".join(methods_es[:-1]) + f" y {methods_es[-1]}"


def add_context_variables(variables: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """Combina variables base con contexto adicional de la conversación."""
    if not variables:
        variables = {}
    if not context:
        return variables
    combined = variables.copy()
    for key, value in context.items():
        combined[f"ctx_{key}"] = value
        if key not in combined:
            combined[key] = value
    return combined
