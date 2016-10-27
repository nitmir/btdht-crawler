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
"""forms for the app"""
from .settings import settings

from django import forms
from django.forms import widgets
from django.utils.translation import ugettext_lazy as _


class BootsrapForm(forms.Form):
    """
        Bases: :class:`django.forms.Form`

        Form base class to use boostrap then rendering the form fields
    """
    def __init__(self, *args, **kwargs):
        super(BootsrapForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            # Only tweak the field if it will be displayed
            if not isinstance(field.widget, widgets.HiddenInput):
                attrs = {}
                if isinstance(field.widget, (widgets.Input, widgets.Select, widgets.Textarea)):
                    attrs['class'] = "form-control"
                if isinstance(field.widget, (widgets.Input, widgets.Textarea)) and field.label:
                    attrs["placeholder"] = field.label
                if field.required:
                    attrs["required"] = "required"
                field.widget.attrs.update(attrs)


class SearchForm(BootsrapForm):
    query = forms.CharField(
        label="",
        widget=forms.TextInput(attrs={
            'autofocus': 'autofocus',
            'placeholder': 'Search here',
        })
    )
