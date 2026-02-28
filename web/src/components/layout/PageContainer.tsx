interface PageContainerProps {
  children: React.ReactNode;
  className?: string;
}

export function PageContainer({ children, className = "" }: PageContainerProps) {
  return (
    <main className={`mx-auto max-w-7xl px-4 py-6 sm:px-6 ${className}`}>
      {children}
    </main>
  );
}
