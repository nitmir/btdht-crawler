# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License version 3 for
# more details.
#
# You should have received a copy of the GNU General Public License version 3
# along with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# (c) 2015-2016 Valentin Samir
"""template tags for the app"""
from django import template
from django import forms

import re

from ..utils import format_size, format_date, absolute_url as utils_absolute_url

register = template.Library()


@register.filter(name='is_checkbox')
def is_checkbox(field):
    """
        check if a form bound field is a checkbox

       :param django.forms.BoundField field: A bound field
       :return: ``True`` if the field is a checkbox, ``False`` otherwise.
       :rtype: bool
    """
    return isinstance(field.field.widget, forms.CheckboxInput)


@register.filter(name='is_radio')
def is_radio(field):
    """
        check if a form bound field is a radio

       :param django.forms.BoundField field: A bound field
       :return: ``True`` if the field is a radio, ``False`` otherwise.
       :rtype: bool
    """
    return isinstance(field.field.widget, forms.RadioSelect)


@register.filter(name='is_hidden')
def is_hidden(field):
    """
        check if a form bound field is hidden

       :param django.forms.BoundField field: A bound field
       :return: ``True`` if the field is hidden, ``False`` otherwise.
       :rtype: bool
    """
    return isinstance(field.field.widget, forms.HiddenInput)


@register.filter(name='size_pp')
def size_pp(size):
    return format_size(size)


@register.filter(name='date_pp')
def date_pp(timestamp):
    return format_date(timestamp)


@register.filter(name='replace')
def replace(value, arg):
    (match, rep) = arg.split(':')
    return value.replace(match, rep)


@register.filter(name='absolute_url')
def absolute_url(path, request):
    return utils_absolute_url(request, path)


class NoSpace(template.Node):
    def __init__(self, nodelist):
        self.nodelist = nodelist

    def render(self, context):
        return self.remove_whitespace(self.nodelist.render(context).strip())

    def remove_whitespace(self, value):
        value = re.sub(r'\n', '', value)
        value = re.sub(r' +', '', value)
        return value


@register.tag(name='nospace')
def nospace(parser, token):
    """
    Remove all whitespace from content
    """
    nodelist = parser.parse(('endnospace',))
    parser.delete_first_token()
    return NoSpace(nodelist)
