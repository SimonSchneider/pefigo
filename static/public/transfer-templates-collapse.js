// Lightweight collapsible rows for transfer templates
(function() {
  'use strict';

  function childRowsOf(templateId) {
    return document.querySelectorAll('tr[data-parent-id="' + templateId + '"]');
  }

  // collapse hides every row below row: its children, and theirs, which collapse too.
  function collapse(row) {
    row.classList.add('collapsed');
    const childRows = childRowsOf(row.getAttribute('data-template-id'));
    for (let i = 0; i < childRows.length; i++) {
      childRows[i].classList.add('hidden');
      if (childRows[i].hasAttribute('data-template-id')) {
        collapse(childRows[i]);
      }
    }
  }

  // expand shows the children of row; a child that is a group stays collapsed.
  function expand(row) {
    row.classList.remove('collapsed');
    const childRows = childRowsOf(row.getAttribute('data-template-id'));
    for (let i = 0; i < childRows.length; i++) {
      childRows[i].classList.remove('hidden');
    }
  }

  function toggleChildren(parentRow) {
    const templateId = parentRow.getAttribute('data-template-id');
    if (!templateId || childRowsOf(templateId).length === 0) return;

    if (parentRow.classList.contains('collapsed')) {
      expand(parentRow);
    } else {
      collapse(parentRow);
    }
  }

  // Called by the small toggle button (kept for backwards compat)
  window.toggleTemplateChildren = function(button) {
    const parentRow = button.closest('tr[data-template-id]');
    if (parentRow) {
      toggleChildren(parentRow);
    }
  };

  // Called when the whole group row is clicked
  window.toggleTemplateRow = function(row) {
    toggleChildren(row);
  };

  function initCollapsibleRows() {
    // Find all parent rows and collapse them initially
    const parentRows = document.querySelectorAll('tr[data-template-id]');

    for (let i = 0; i < parentRows.length; i++) {
      const parentRow = parentRows[i];
      const templateId = parentRow.getAttribute('data-template-id');
      if (!templateId) continue;

      const childRows = document.querySelectorAll('tr[data-parent-id="' + templateId + '"]');

      // Set initial state - collapsed by default
      if (childRows.length > 0) {
        parentRow.classList.add('collapsed');
        for (let j = 0; j < childRows.length; j++) {
          childRows[j].classList.add('hidden');
        }
      }
    }
  }

  // Initialize immediately and on DOM ready
  initCollapsibleRows();

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCollapsibleRows);
  }
})();
