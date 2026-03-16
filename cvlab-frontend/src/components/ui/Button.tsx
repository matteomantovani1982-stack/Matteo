import { ButtonHTMLAttributes, forwardRef } from 'react'
import s from './Button.module.css'

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'ghost' | 'danger'
  size?: 'sm' | 'md'
  loading?: boolean
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ variant = 'primary', size = 'md', loading, children, disabled, className = '', ...rest }, ref) => {
    return (
      <button
        ref={ref}
        disabled={disabled || loading}
        className={[s.btn, s[variant], s[size], className].join(' ')}
        {...rest}
      >
        {loading && <span className={s.spinner} />}
        {children}
      </button>
    )
  }
)
Button.displayName = 'Button'
