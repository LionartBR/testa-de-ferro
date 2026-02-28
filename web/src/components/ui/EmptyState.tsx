interface EmptyStateProps {
  message?: string;
}

export function EmptyState({
  message = "Nenhum resultado encontrado.",
}: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-center">
      <p className="text-sm text-gray-500">{message}</p>
    </div>
  );
}
