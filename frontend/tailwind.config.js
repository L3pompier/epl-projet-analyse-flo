/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./*.html", "./js/**/*.js"],
  theme: {
    extend: {
      fontFamily: {
        sora: ['Sora', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
      colors: {
        deep: '#0d1117', card: '#161b27', card2: '#1c2333',
        border: '#2a3050', accent: '#6366f1'
      }
    }
  },
  plugins: [],
}
