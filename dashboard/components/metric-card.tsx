export function MetricCard({
  label,
  value,
  accent,
}: {
  label: string;
  value: string | number;
  accent: "amber" | "blue" | "green" | "red";
}) {
  return (
    <article className={`metric-card metric-${accent}`}>
      <p>{label}</p>
      <strong>{value}</strong>
    </article>
  );
}
