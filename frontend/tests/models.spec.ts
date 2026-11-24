import { test, expect } from '@playwright/test'
import { gotoAndEnsureAuth, waitForGraphQLOperation } from './auth.utils'

test.describe('Admin Models Management', () => {
  test.beforeEach(async ({ page }) => {
    test.setTimeout(60000)
    await gotoAndEnsureAuth(page, '/models')

    const modelsTable = page.getByTestId('models-table')
    await modelsTable.waitFor({ state: 'visible', timeout: 20000 })

    // Handle driver.js onboarding overlay (it has allowClose: false, so Escape won't work)
    const driverOverlay = page.locator('#driver-popover-content')
    if (await driverOverlay.isVisible().catch(() => false)) {
      // Click the highlighted settings button to dismiss the onboarding
      const settingsButton = page.locator('[data-settings-button]')
      if (await settingsButton.isVisible().catch(() => false)) {
        await settingsButton.click()
        await page.waitForTimeout(500)
      }
      // Wait for driver overlay to disappear
      await expect(driverOverlay).not.toBeVisible({ timeout: 5000 }).catch(() => {})
    }

    // Close any dialog that may have opened (e.g., settings dialog from clicking the button)
    const settingsDialog = page.getByRole('dialog').filter({ hasText: /Model Settings|模型设置/i })
    if (await settingsDialog.isVisible().catch(() => false)) {
      await page.keyboard.press('Escape')
      await expect(settingsDialog).not.toBeVisible({ timeout: 5000 })
    }
  })

  test('can create, edit, filter, toggle status, and delete a model', async ({ page }) => {
    const uniqueSuffix = Date.now().toString().slice(-6)
    const baseName = `pw-model-${uniqueSuffix}`
    const updatedName = `${baseName}-updated`

    // Open create dialog
    const createButton = page
      .getByRole('button', { name: /Add Model|创建模型|新增模型|创建|Add/i })
      .first()
    await expect(createButton).toBeVisible()
    await createButton.click()

    const dialog = page.locator('[data-slot="dialog-content"]')
    await expect(dialog).toBeVisible()

    // Select developer
    const developerCombo = dialog.locator('[role="combobox"]').first()
    await developerCombo.click()
    const developerOption = page
      .getByRole('option', { name: /Moonshot AI|Moonshot/i })
      .or(page.getByRole('option', { name: /OpenAI/i }))
      .first()
    await developerOption.click()

    // Select a modelId from provider list
    const modelIdInput = dialog.getByPlaceholder(/model id/i).first()
    await modelIdInput.click()
    await modelIdInput.fill('kimi')
    const modelOption = page.getByRole('option', { name: /kimi-k2-thinking/i }).first()
    await expect(modelOption).toBeVisible()
    await modelOption.click()

    // Override default name/group with deterministic values
    const nameInput = dialog.getByLabel(/Name|名称/i)
    await nameInput.fill(baseName)
    const groupInput = dialog.getByLabel(/Group|分组/i)
    await groupInput.fill(`group-${uniqueSuffix}`)
    const remarkInput = dialog.getByLabel(/Remark|备注/i)
    if (await remarkInput.count()) {
      await remarkInput.fill('Created via Playwright E2E')
    }

    await Promise.all([
      waitForGraphQLOperation(page, 'CreateModel'),
      dialog.getByRole('button', { name: /Create|创建|保存|Save/i }).last().click(),
    ])
    await expect(dialog).not.toBeVisible({ timeout: 20000 })
    await waitForGraphQLOperation(page, 'GetModels')

    const modelsTable = page.getByTestId('models-table')
    const createdRow = modelsTable.locator('tbody tr').filter({ hasText: baseName })
    await expect(createdRow).toBeVisible({ timeout: 20000 })

    // Edit the created model
    const rowActions = createdRow.getByTestId('row-actions').first()
    await rowActions.click()
    const editMenuItem = page.getByRole('menuitem', { name: /Edit|编辑/i }).first()
    await editMenuItem.click()

    const editDialog = page.getByRole('dialog').filter({ hasText: /Edit Model|编辑/i }).first()
    await expect(editDialog).toBeVisible()
    const editNameInput = editDialog.getByLabel(/Name|名称/i)
    await editNameInput.fill(updatedName)

    await Promise.all([
      waitForGraphQLOperation(page, 'UpdateModel'),
      editDialog.getByRole('button', { name: /Save|保存|Update|更新/i }).last().click(),
    ])
    await expect(editDialog).not.toBeVisible({ timeout: 20000 })
    await waitForGraphQLOperation(page, 'GetModels')

    const updatedRow = modelsTable.locator('tbody tr').filter({ hasText: updatedName })
    await expect(updatedRow).toBeVisible({ timeout: 20000 })

    // Verify filtering by name works with the updated name
    const filterInput = page.getByPlaceholder(/Filter by name|名称|搜索/i)
    await filterInput.fill(updatedName)
    await page.waitForTimeout(800)
    await expect(updatedRow).toBeVisible()
    await filterInput.fill('')
    await page.waitForTimeout(400)

    // Toggle status via switch (enable/disable)
    const statusSwitch = updatedRow.locator('[data-testid="model-status-switch"]').first()
    await statusSwitch.click()
    const statusDialog = page.getByRole('alertdialog').or(page.getByRole('dialog'))
    await expect(statusDialog).toBeVisible()
    const confirmStatusButton = statusDialog
      .getByRole('button', { name: /Confirm|确认|确定|Enable|Disable/i })
      .last()
    await Promise.all([
      waitForGraphQLOperation(page, 'UpdateModel'),
      confirmStatusButton.click(),
    ])
    await expect(statusDialog).not.toBeVisible({ timeout: 20000 })
    await waitForGraphQLOperation(page, 'GetModels')

    // Delete the created model
    const actionsAfterToggle = updatedRow.getByTestId('row-actions').first()
    await actionsAfterToggle.click()
    const deleteMenuItem = page.getByRole('menuitem', { name: /Delete|删除/i }).first()
    await deleteMenuItem.click()

    const deleteDialog = page.getByRole('alertdialog').or(page.getByRole('dialog'))
    await expect(deleteDialog).toBeVisible()
    const deleteButton = deleteDialog.getByRole('button', { name: /Delete|删除|Confirm|确认/i }).last()
    await Promise.all([
      waitForGraphQLOperation(page, 'DeleteModel'),
      deleteButton.click(),
    ])
    await expect(deleteDialog).not.toBeVisible({ timeout: 20000 })
    await waitForGraphQLOperation(page, 'GetModels')

    await expect(modelsTable.locator('tbody tr').filter({ hasText: updatedName })).toHaveCount(0)
  })

  test('can create custom model IDs with a new or existing group', async ({ page }) => {
    const prefix = `pw-custom-${Date.now()}`
    const group = `${prefix}-group`
    const createdIds: string[] = []

    try {
      for (const useExistingGroup of [false, true]) {
        const modelID = `${prefix}-${useExistingGroup ? 'existing' : 'new'}`
        await page.getByRole('button', { name: /^(Add Model|添加模型)$/ }).click()
        const dialog = page.getByRole('dialog')
        await dialog.getByPlaceholder(/Select developer|选择开发者/).fill('OpenAI')
        await page.getByRole('option', { name: 'OpenAI', exact: true }).click()
        await dialog.getByPlaceholder(/Enter or select model ID|输入或选择模型 ID/).fill(modelID)
        await dialog.getByLabel(/^(Name|名称)$/).fill(modelID)

        const groupInput = dialog.getByLabel(/^(Group|分组)$/)
        await groupInput.fill(useExistingGroup ? prefix : group)
        if (useExistingGroup) {
          await page.getByRole('option', { name: group, exact: true }).click()
          await expect(groupInput).toHaveValue(group)
          await dialog.getByLabel(/^(Cache Read|缓存读取)$/).fill('0.005')
        }

        const [response] = await Promise.all([
          page.waitForResponse((response) => {
            const request = response.request()
            return request.url().endsWith('/admin/graphql') &&
              request.method() === 'POST' && request.postDataJSON().operationName === 'CreateModel'
          }),
          dialog.getByRole('button', { name: /^(Create|创建)$/ }).click(),
        ])
        const result = await response.json()
        if (result.data?.createModel?.id) createdIds.push(result.data.createModel.id)
        expect(result.errors).toBeUndefined()

        const { input } = response.request().postDataJSON().variables
        expect(input).toMatchObject({ modelID, group })
        if (useExistingGroup) expect(input.modelCard.cost).toMatchObject({ cacheRead: 0.005 })
        await expect(dialog).not.toBeVisible()
      }
    } finally {
      const token = await page.evaluate(() => localStorage.getItem('axonhub_access_token'))
      for (const id of createdIds) {
        const response = await page.request.post('/admin/graphql', {
          headers: { Authorization: `Bearer ${token}` },
          data: { query: 'mutation DeleteModel($id: ID!) { deleteModel(id: $id) }', variables: { id } },
        })
        expect(await response.json()).toMatchObject({ data: { deleteModel: true } })
      }
    }
  })
})
