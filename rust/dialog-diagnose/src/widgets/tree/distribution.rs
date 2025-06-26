pub use ratatui::prelude::*;
use ratatui::widgets::{Axis, Block, Chart, Dataset, GraphType};

use crate::ArtifactsTreeStats;

pub struct DistributionChart<'a> {
    pub stats: &'a ArtifactsTreeStats,
}

impl Widget for DistributionChart<'_> {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        let stats = self.stats;
        let min_x = stats.minimum_segment_size;
        let max_x = stats.maximum_segment_size;
        let mut min_y = None;
        let mut max_y = 0f64;

        let data = stats
            .distribution
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let i = i as f64;
                let v = *v as f64;

                min_y = min_y.or(Some(v)).map(|current| current.min(v));
                max_y = max_y.max(v);

                (i, v)
            })
            .collect::<Vec<(f64, f64)>>();

        let min_y = min_y.unwrap_or_default();

        let dataset = Dataset::default()
            .name("Segment distribution")
            .marker(symbols::Marker::Braille)
            .style(Style::default().fg(Color::Yellow))
            .graph_type(GraphType::Line)
            .data(&data);

        Chart::new(vec![dataset])
            .block(Block::new().title(Line::from("Distribution").cyan().bold().centered()))
            .x_axis(
                Axis::default()
                    .title("Entries / Segment")
                    .style(Style::default().gray())
                    .bounds([0., 10.])
                    .labels([
                        format!("{:.0}", min_x).bold(),
                        format!("{:.0}", (max_x - min_x) / 2 + min_x).into(),
                        format!("{:.0}", max_x).bold(),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .title("# Segments")
                    .style(Style::default().gray())
                    .bounds([min_y, max_y])
                    .labels([
                        format!("{:.0}", min_y).bold(),
                        format!("{:.0}", min_y + (max_y - min_y) / 2.).into(),
                        format!("{:.0}", max_y).bold(),
                    ]),
            )
            .legend_position(None)
            .render(area, buf);
    }
}
