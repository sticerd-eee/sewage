#show link: set text(fill: rgb("#1F3A93"))
#show ref: set text(fill: rgb("#1F3A93"))
#show heading.where(level: 1): set block(above: 1.8em, below: 1em)
#show heading.where(level: 2): set block(above: 1.6em, below: 1em)
#show heading.where(level: 3): set block(above: 1.5em, below: 0.95em)
#show figure: set block(above: 1.8em, below: 1.8em)
#show figure: set figure(gap: 0.8em)
#show table: set par(justify: false)
#show table: set text(hyphenate: false)
#let figdepth = state("figdepth", 0)
#show figure: it => {
  figdepth.update(d => d + 1)
  context {
    if figdepth.get() > 1 {
      set text(weight: "regular")
      it
    } else {
      it
    }
  }
  figdepth.update(d => d - 1)
}
#show figure.caption: it => context {
  if figdepth.get() > 1 { it } else { text(weight: "bold", it) }
}
