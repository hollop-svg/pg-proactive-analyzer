document.addEventListener('DOMContentLoaded', () => {
  const menuEl = document.getElementById('menu');
  if (!menuEl) return;

  const menuHTML = `
    <nav>
      <ul>
        <li><a href="index.html">Анализ запроса</a></li>
        <li><a href="history.html">История</a></li>
        <li><a href="info_BD.html">Информация о базе данных</a></li>
         <li><a href="heatmap.html">Аналитика</a></li>
      </ul>
    </nav>
  `;

  menuEl.innerHTML = menuHTML;

  // Подсветка активного пункта
  const links = menuEl.querySelectorAll('a');
  const current = (location.pathname.split('/').pop() || 'index.html').toLowerCase();

  links.forEach(a => {
    const href = (a.getAttribute('href') || '').split('/').pop().toLowerCase();
    a.parentElement.classList.toggle('active', href === current);
  });
});